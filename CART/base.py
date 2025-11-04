from neo4j import GraphDatabase
import json
import time
import os
import socket
import shutil
import subprocess
import getpass
import ipaddress
from typing import Optional

# Imports used by build_database helpers
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

class Neo4jConnection:
    """
    A base class to handle Neo4j database connections and container management.
    
    This class manages both the container lifecycle (start, stop, remove) and
    database connections, allowing other analyzer classes to inherit these capabilities.
    """
    def __init__(self, uri: Optional[str] = None, user: str = "neo4j", password: Optional[str] = None,
                 database: str = "neo4j", container_name: str = "neo4j", 
                 neo4j_image: str = "neo4j:latest", bolt_port: int = 7687, 
                 heap_initial: str = "2G", heap_max: str = "4G"):
        """
        Initializes the connection and container parameters.
        
        Args:
            uri (str): The Bolt URI for the Neo4j instance
            user (str): The username for the Neo4j instance
            password (str): The password for the Neo4j instance
            database (str): The database name
            container_name (str): Name for the Neo4j container
            neo4j_image (str, optional): Docker image to use (default: neo4j:latest)
            bolt_port (int, optional): Port for Bolt protocol (default: 7687)
            heap_initial (str, optional): Initial heap size (default: 2G)
            heap_max (str, optional): Maximum heap size (default: 4G)
        """
        # Container configuration
        self.container_name = container_name
        self.neo4j_image = neo4j_image
        self.bolt_port = bolt_port
        self.heap_initial = heap_initial
        self.heap_max = heap_max
        self.http_port = None
        self._docker_path = None
        # None = unknown, False = don't use sudo, True = prefix docker calls with sudo
        self._use_sudo = None
        # Cached sudo password (None = not provided / passwordless sudo)
        self._sudo_password = None
        
        # Connection configuration
        self.uri = uri or f"bolt://localhost:{bolt_port}"
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        print(f"{self.__class__.__name__} initialized.")

    def status(self) -> dict:
        """Return a small status summary for the connection/container.

        Does NOT attempt to open the driver or prompt for passwords. Use
        this to inspect whether the container exists and if it's running.
        """
        exists = None
        running = None
        try:
            exists = self._container_exists()
        except Exception:
            exists = None
        try:
            running = self._is_container_running()
        except Exception:
            running = None

        status = {
            'container_name': getattr(self, 'container_name', None),
            'neo4j_image': getattr(self, 'neo4j_image', None),
            'exists': exists,
            'running': running,
            'http_port': getattr(self, 'http_port', None),
            'bolt_port': getattr(self, 'bolt_port', None),
            'driver_connected': self.driver is not None
        }

        # Print a concise one-line summary for interactive use
        exist_text = 'exists' if exists else 'missing'
        run_text = 'running' if running else ('stopped' if running is False else 'unknown')
        print(f"Container '{status['container_name']}': {exist_text}, {run_text}; driver_connected={status['driver_connected']}")
        return status

    def connect(self):
        """Establishes a connection to the Neo4j database."""
        if self.driver is None:
            try:
                if not self.password:
                    print("No password set. Please start the container first or provide a password.")
                    return False
                    
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                self.driver.verify_connectivity()
                print("✓ Successfully connected to the Neo4j database.")
            except Exception as e:
                print(f"Failed to connect to Neo4j: {e}")
                self.driver = None
                
                # Check if this might be a container issue
                is_running = self._is_container_running()
                if is_running is False:
                    print("The Neo4j container appears to be stopped. Try calling start() first.")
                elif is_running is None:
                    print("Could not determine if Neo4j container is running. Try calling start().")
                    
        return self.driver is not None

    def close(self):
        """Closes the connection to the Neo4j database."""
        if self.driver is not None:
            self.driver.close()
            self.driver = None
            print("✓ Neo4j connection closed.")

    def _check_port_available(self, port: int) -> bool:
        """Check if a port is available."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return True
            except socket.error:
                return False

    def _ipv4_to_subnet(self, ip_address: Optional[str], prefix_len: int = 24) -> Optional[str]:
        """Convert an IPv4 address string into a canonical subnet (e.g., 192.168.1.0/24)."""
        if not ip_address:
            return None
        try:
            addr = ipaddress.ip_address(ip_address)
            if isinstance(addr, ipaddress.IPv4Address):
                network = ipaddress.ip_network(f"{ip_address}/{prefix_len}", strict=False)
                return str(network)
        except ValueError:
            return None
        return None

    def _get_available_http_port(self) -> Optional[int]:
        """Get an available HTTP port (tries 7474 then 8080)."""
        if self._check_port_available(7474):
            print("Browser port 7474 is available.")
            return 7474
        print("Browser port 7474 is IN USE.")
        if self._check_port_available(8080):
            return 8080
        print("FATAL ERROR: Both ports 7474 and 8080 are in use.")
        return None

    def _ensure_docker_available(self) -> None:
        """Ensure Docker CLI is available and responding (uses `docker image ls`).

        Raises RuntimeError when Docker is missing or the daemon is not responding.
        """
        # Try the simple call first (works when 'docker' is on PATH)
        try:
            proc = subprocess.run(["docker", "image", "ls"], capture_output=True)
            if proc.returncode == 0:
                self._docker_path = shutil.which("docker") or "docker"
                self._use_sudo = False
                self._sudo_password = None
                return
            # If docker exists but returned non-zero, we'll try to detect if sudo helps below
        except FileNotFoundError:
            proc = None

        # If the simple call didn't succeed, try locating docker via shutil.which
        docker_path = shutil.which("docker")
        if docker_path:
            # try running with absolute path without sudo
            try:
                proc2 = subprocess.run([docker_path, "image", "ls"], capture_output=True)
                if proc2.returncode == 0:
                    self._docker_path = docker_path
                    self._use_sudo = False
                    self._sudo_password = None
                    return
                # If non-zero, try with sudo to see if the daemon is permission-locked.
                # First try passwordless sudo (-n). If that fails, prompt the user for a sudo password
                # and try again with -S, supplying the password via stdin.
                try:
                    proc2_sudo = subprocess.run(["sudo", "-n", docker_path, "image", "ls"], capture_output=True, text=True)
                    if proc2_sudo.returncode == 0:
                        self._docker_path = docker_path
                        self._use_sudo = True
                        self._sudo_password = None
                        return
                except FileNotFoundError:
                    proc2_sudo = None

                try:
                    sudo_pw = getpass.getpass("sudo password required to run docker commands (will be used for this session): ")
                    if sudo_pw:
                        proc2_sudo_pw = subprocess.run(["sudo", "-S", docker_path, "image", "ls"], input=sudo_pw + "\n", capture_output=True, text=True)
                        if proc2_sudo_pw.returncode == 0:
                            self._docker_path = docker_path
                            self._use_sudo = True
                            self._sudo_password = sudo_pw
                            return
                        else:
                            stderr = (proc2.stderr or "") + "\n" + (proc2_sudo_pw.stderr or "")
                            stderr = stderr.strip()
                            raise RuntimeError(f"Docker appears to be installed but not usable: {stderr}")
                except Exception:
                    # fall through to try other locations
                    pass
            except FileNotFoundError:
                pass

        # Try common locations (typical paths)
        common = [
            "/usr/local/bin/docker",
            "/opt/homebrew/bin/docker",
            "/usr/bin/docker",
        ]
        for p in common:
            if os.path.exists(p) and os.access(p, os.X_OK):
                try:
                    proc3 = subprocess.run([p, "image", "ls"], capture_output=True)
                    if proc3.returncode == 0:
                        self._docker_path = p
                        self._use_sudo = False
                        self._sudo_password = None
                        return
                    # try passwordless sudo first
                    try:
                        proc3_sudo = subprocess.run(["sudo", "-n", p, "image", "ls"], capture_output=True, text=True)
                        if proc3_sudo.returncode == 0:
                            self._docker_path = p
                            self._use_sudo = True
                            self._sudo_password = None
                            return
                    except FileNotFoundError:
                        proc3_sudo = None

                    # Prompt for sudo password if passwordless sudo didn't work
                    try:
                        sudo_pw = getpass.getpass("sudo password required to run docker commands (will be used for this session): ")
                        if sudo_pw:
                            proc3_sudo_pw = subprocess.run(["sudo", "-S", p, "image", "ls"], input=sudo_pw + "\n", capture_output=True, text=True)
                            if proc3_sudo_pw.returncode == 0:
                                self._docker_path = p
                                self._use_sudo = True
                                self._sudo_password = sudo_pw
                                return
                    except Exception:
                        pass
                except FileNotFoundError:
                    continue

        # Nothing worked
        raise RuntimeError("Docker CLI not found or not usable. Please install Docker or ensure 'docker' is on PATH and the daemon is running, or that your user is in the 'docker' group.")

    def _docker_run(self, args, **kwargs):
        """Run a docker command with resolved executable path."""
        if not getattr(self, "_docker_path", None):
            self._ensure_docker_available()
        kwargs = dict(kwargs)

        if self._use_sudo:
            if self._sudo_password:
                sudo_prefix = ["sudo", "-S"]
                password_input = (self._sudo_password + "\n").encode()
                if "input" in kwargs and kwargs["input"] is not None:
                    existing_input = kwargs["input"]
                    if isinstance(existing_input, str):
                        existing_input = existing_input.encode()
                    kwargs["input"] = password_input + existing_input
                else:
                    kwargs["input"] = password_input
                kwargs.pop("text", None)
                kwargs.pop("encoding", None)
            else:
                sudo_prefix = ["sudo", "-n"]
                if "input" in kwargs and kwargs["input"] is None:
                    kwargs.pop("input")
            cmd = sudo_prefix + [self._docker_path] + args
        else:
            cmd = [self._docker_path] + args

        return subprocess.run(cmd, **kwargs)

    def _docker_check_output(self, args, **kwargs):
        """Run docker command and return output (like check_output)."""
        if not getattr(self, "_docker_path", None):
            self._ensure_docker_available()
        kwargs = dict(kwargs)

        if self._use_sudo:
            if self._sudo_password:
                sudo_prefix = ["sudo", "-S"]
                password_input = (self._sudo_password + "\n").encode()
                if "input" in kwargs and kwargs["input"] is not None:
                    existing_input = kwargs["input"]
                    if isinstance(existing_input, str):
                        existing_input = existing_input.encode()
                    kwargs["input"] = password_input + existing_input
                else:
                    kwargs["input"] = password_input
                kwargs.pop("text", None)
                kwargs.pop("encoding", None)
            else:
                sudo_prefix = ["sudo", "-n"]
                if "input" in kwargs and kwargs["input"] is None:
                    kwargs.pop("input")
            cmd = sudo_prefix + [self._docker_path] + args
        else:
            cmd = [self._docker_path] + args

        return subprocess.check_output(cmd, **kwargs)

    def _get_container_logs(self, tail: Optional[int] = None) -> str:
        """Return recent container logs as string (uses docker logs).

        If tail is provided, will pass --tail <n> to docker logs when supported.
        """
        try:
            args = ["logs", self.container_name]
            if tail is not None:
                args.extend(["--tail", str(tail)])
            out = self._docker_check_output(args, stderr=subprocess.STDOUT)
            return out.decode(errors='replace')
        except Exception as e:
            return f"Could not retrieve docker logs: {e}"

    def _get_container_ports(self) -> str:
        """Return the docker 'Ports' string for the container (empty on error).

        Example output: '0.0.0.0:7474->7474/tcp, 0.0.0.0:7687->7687/tcp'
        """
        try:
            out = self._docker_check_output([
                "ps", "--filter", f"name=^{self.container_name}$", "--format", "{{.Ports}}"
            ], stderr=subprocess.STDOUT)
            return out.decode(errors='replace').strip()
        except Exception:
            return ""

    def _wait_for_bolt(self, timeout: int = 120, interval: float = 3.0) -> bool:
        """Wait until the Bolt port responds and verify connectivity.

        Tries to establish a TCP connection to localhost:self.bolt_port and then
        calls the driver's verify_connectivity(). Returns True on success, False on timeout.
        """
        end = time.time() + timeout
        last_exc = None
        while time.time() < end:
            # quick TCP probe
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2.0)
                try:
                    sock.connect(('localhost', int(self.bolt_port)))
                except Exception as e:
                    last_exc = e
                    time.sleep(interval)
                    continue

            # TCP connect succeeded, try driver verify
            try:
                drv = GraphDatabase.driver(f"bolt://localhost:{self.bolt_port}", auth=(self.user, self.password))
                drv.verify_connectivity()
                drv.close()
                return True
            except Exception as e:
                last_exc = e
                # wait and retry
                time.sleep(interval)
                continue

        # timed out
        print(f"Timed out waiting for Bolt on port {self.bolt_port}: {last_exc}")
        # print a short tail of container logs to help debugging
        logs = self._get_container_logs(tail=200)
        print("---- Container logs (last lines) ----")
        print(logs)
        print("---- end logs ----")
        return False

    def _container_exists(self) -> bool:
        """Return True if a container with self.container_name exists (any state)."""
        try:
            # Use exact-name filter to find container ID by name (safer than partial matches)
            cmd = ["ps", "-aq", "--filter", f"name=^{self.container_name}$"]
            res = self._docker_run(cmd, capture_output=True, check=False)
            out = res.stdout.decode().strip()
            if out:
                return True
            # Fallback to inspect if ps didn't find it (edge cases)
            try:
                res2 = self._docker_run(["inspect", self.container_name], capture_output=True)
                return res2.returncode == 0
            except FileNotFoundError:
                return False
        except FileNotFoundError:
            return False

    def _is_container_running(self) -> Optional[bool]:
        """Return True if container is running, False if not running, None if unknown."""
        try:
            # Check running state by filtering running containers by exact name
            cmd = ["ps", "-q", "--filter", f"name=^{self.container_name}$"]
            out = self._docker_check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
            if out:
                return True
            # If no running container matched, but a container may exist in stopped state
            exists = self._container_exists()
            if exists:
                return False
            return None
        except subprocess.CalledProcessError:
            return None

    def _cleanup_existing_containers(self):
        """Stop and remove any existing Neo4j containers."""
        try:
            # Find existing containers (by image)
            cmd = ["ps", "-a", "-q", "--filter", f"ancestor={self.neo4j_image}"]
            containers = self._docker_check_output(cmd).decode().strip()

            if containers:
                print("Cleaning up existing Neo4j container(s)...")
                for container_id in containers.split():
                    self._docker_run(["stop", container_id], check=True, capture_output=True)
                    self._docker_run(["rm", "-f", container_id], check=True, capture_output=True)
                print("Cleanup complete.")
            else:
                print("No old containers found.")
        except subprocess.CalledProcessError as e:
            print(f"Error during cleanup: {e}")
            raise

    def start(self, password: Optional[str] = "ubuntuubuntu") -> bool:
        """
        Start the Neo4j container.
        
        Args:
            password: Optional password for Neo4j user. If not provided, will prompt.
            
        Returns:
            bool: True if container started successfully, False otherwise
        """
        # Get password if not provided
        if not password:
            while True:
                password = getpass.getpass(
                    f"Enter the desired password for the '{self.user}' user (min 8 chars): "
                )
                if len(password) >= 8:
                    break
                print("ERROR: Password must be at least 8 characters long. Please try again.")
        self.password = password

        # Get available HTTP port
        self.http_port = self._get_available_http_port()
        if not self.http_port:
            return False

        # Ensure Docker is installed and usable
        try:
            self._ensure_docker_available()
        except RuntimeError:
            raise

        # If a container with the desired name is already running and exposes
        # the standard HTTP port (7474), don't start a second container.
        try:
            if self._is_container_running():
                ports = self._get_container_ports()
                if ports and '7474' in ports:
                    # Map http_port to 7474 for reporting convenience
                    self.http_port = 7474
                    print(f"Container '{self.container_name}' is already running and exposes HTTP on port 7474. No new container started.")
                    return True
        except Exception:
            # If we couldn't determine running state, continue with normal start flow
            pass

        # Ensure the neo4j image exists locally; if not, pull it
        try:
            cmd_check = ["images", "-q", self.neo4j_image]
            proc = self._docker_run(cmd_check, capture_output=True, check=False)
            image_id = proc.stdout.decode().strip()
            if not image_id:
                print(f"Docker image '{self.neo4j_image}' not found locally. Pulling...")
                pull_proc = self._docker_run(["pull", self.neo4j_image], capture_output=True)
                if pull_proc.returncode != 0:
                    print(f"Failed to pull image '{self.neo4j_image}': {pull_proc.stderr.decode().strip()}")
                    return False
                print(f"Successfully pulled '{self.neo4j_image}'.")
            else:
                print(f"Docker image '{self.neo4j_image}' found locally: {image_id}")
        except FileNotFoundError:
            raise RuntimeError("Docker CLI not found. Please install Docker or ensure 'docker' is on PATH.")

        # Cleanup existing containers
        try:
            self._cleanup_existing_containers()
        except Exception as e:
            print(f"Failed to cleanup existing containers: {e}")
            return False

        # Start new container
        print(f"Launching new Neo4j container: '{self.container_name}' with GDS Plugin...")
        try:
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.http_port}:7474",
                "-p", f"{self.bolt_port}:7687",
                "-e", f"NEO4J_AUTH={self.user}/{self.password}",
                "-e", 'NEO4J_PLUGINS=["graph-data-science"]',
                "--env", f"NEO4J_dbms_memory_heap_initial__size={self.heap_initial}",
                "--env", f"NEO4J_dbms_memory_heap_max__size={self.heap_max}",
                self.neo4j_image
            ]
            self._docker_run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"FATAL ERROR: Docker failed to start the Neo4j container: {e}")
            return False

        print("Waiting for Neo4j Bolt to become available (this may take up to 2 minutes)...")
        # Wait and verify connectivity (longer timeout than fixed sleep)
        if not self._wait_for_bolt(timeout=120, interval=3):
            print("❌ Failed to connect to Neo4j after waiting for Bolt handshake. See logs above.")
            return False

        print("\n" + "=" * 58)
        print("Deployment Complete.")
        print(f"   - Container Name: {self.container_name}")
        print(f"   - **Access Browser at:** http://localhost:{self.http_port}")
        print("=" * 58)
        
        return True

    def stop(self) -> bool:
        """
        Stop the Neo4j container.
        
        Returns:
            bool: True if container stopped successfully, False otherwise
        """
        # If the container doesn't exist, consider it stopped
        exists = self._container_exists()
        if not exists:
            print(f"Container '{self.container_name}' does not exist (already stopped).")
            return True

        is_running = self._is_container_running()
        if is_running is False:
            print(f"Container '{self.container_name}' is not running.")
            return True
        if is_running is None:
            # Could not determine state; try stopping and let docker return a helpful error
            print("Could not determine container running state; attempting to stop anyway.")

        try:
            self._docker_run(["stop", self.container_name], check=True, capture_output=True)
            print(f"Container '{self.container_name}' stopped successfully.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to stop container: {e}")
            return False

    def remove(self) -> bool:
        """
        Remove the Neo4j container.
        
        Returns:
            bool: True if container removed successfully, False otherwise
        """
        # If the container is running, stop it first (cleaner than force-remove)
        exists = self._container_exists()
        if not exists:
            print(f"Container '{self.container_name}' does not exist; nothing to remove.")
            return True

        is_running = self._is_container_running()
        if is_running is True:
            print(f"Container '{self.container_name}' is running; stopping it before removal...")
            if not self.stop():
                print("Failed to stop container; aborting remove.")
                return False
        elif is_running is None:
            print("Could not determine container running state; attempting removal anyway.")

        try:
            self._docker_run(["rm", "-f", self.container_name], check=True, capture_output=True)
            print(f"Container '{self.container_name}' removed successfully.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to remove container: {e}")
            return False

    def restart(self) -> bool:
        """
        Restart the Neo4j container.
        
        Returns:
            bool: True if container restarted successfully, False otherwise
        """
        if not self.stop():
            return False
        time.sleep(2)  # Give it a moment
        return self.start(self.password)

    # ==============================================================================
    # === DATABASE BUILDING METHODS ================================================
    # ==============================================================================

    def build_database(self, rebuild=True):
        """
        Orchestrates the entire database construction process.
        
        Args:
            rebuild (bool, optional): If True (default), the existing database will be
                                      wiped clean before loading new data. If False,
                                      new data will be added to the existing graph.
        """

        # Download and prepare data
        self.df = self._download_and_prepare_data()
        if self.df is None:
            print("Failed to prepare data. Aborting.")
            return
        
        try:
            if self.connect():
                self._write_dataframe_to_neo4j(self.df, rebuild=rebuild)
                print("\nDatabase build process finished successfully.")
        finally:
            self.close()

    def _download_and_prepare_data(self):
        """Downloads Zeek data and returns as DataFrame."""
        print("Step 1: Loading and Preparing Data...")
        BASE_URL = "https://datasets.uwf.edu/data/UWF-ZeekData24/parquet/"
        all_dataframes = []
        
        try:
            directory_urls = self._get_directory_urls(BASE_URL)
            
            # Process directories with tqdm
            for dir_url in tqdm(directory_urls, desc="Processing directories", unit="dir"):
                response = requests.get(dir_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                parquet_urls = [urljoin(dir_url, link.get('href')) 
                              for link in soup.find_all('a') 
                              if link.get('href').endswith('.parquet')]
                
                # Process parquet files with nested tqdm
                for url in tqdm(parquet_urls, 
                              desc=f"Loading {dir_url.split('/')[-2]} files", 
                              unit="file",
                              leave=False):  # Don't leave inner progress bars
                    all_dataframes.append(pd.read_parquet(url, engine='pyarrow'))
            
            if not all_dataframes:
                raise ValueError("No dataframes were loaded.")
            
            print("\nCombining and cleaning data...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            cleaned_df = combined_df[combined_df['label_technique'] != 'Duplicate'].copy()
            
            print(f"Prepared {len(cleaned_df):,} rows for import.")
            return cleaned_df
            
        except Exception as e:
            print(f"Error during data preparation: {e}")
            return None

    def _get_directory_urls(self, base_url):
        """Helper to fetch subdirectory URLs."""
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return [urljoin(base_url, link.get('href')) 
                for link in soup.find_all('a') 
                if link.get('href') and link.get('href').startswith('2024') 
                and link.get('href').endswith('/')]

    def _write_dataframe_to_neo4j(self, df, rebuild=True):
            """Write DataFrame directly to Neo4j - optimized for large datasets."""
            print("\nStep 2: Writing Data to Neo4j...")
            
            # The session 'with' block MUST wrap ALL database operations
            with self.driver.session(database=self.database) as session:
                if rebuild:
                    print("Rebuild flag is True. Clearing database in batches...")
                    
                    # Use tqdm for clearing, as discussed
                    with tqdm(unit=" nodes", desc="Clearing database") as pbar:
                        while True:
                            result = session.run("""
                                MATCH (n)
                                WITH n LIMIT 10000
                                DETACH DELETE n
                                RETURN count(n) as deleted
                            """)
                            deleted = result.single()["deleted"]
                            pbar.update(deleted)
                            if deleted == 0:
                                break
                    print(f"  ✓ Database cleared ({pbar.n:,} nodes total)")
                
                # Create index FIRST (critical for performance)
                print("\nCreating indexes...")
                session.run("CREATE INDEX ip_address_index IF NOT EXISTS FOR (n:IP) ON (n.address)")
                print("  ✓ Index created for IP addresses")
                
                # --- All the writing logic is now INSIDE the session block ---
                
                # Prepare data
                total_rows = len(df)
                if total_rows == 0:
                    print("DataFrame is empty. No data to write.")
                    return # Nothing else to do

                batch_size = 15000  # Optimal batch size for Neo4j
                
                print(f"\nWriting {total_rows:,} rows in batches of {batch_size:,}...")
                
                start_time = time.time() # Start timing just before the ingestion
                
                with tqdm(total=total_rows, unit="row", desc="Ingesting data") as pbar:
                    for i in range(0, total_rows, batch_size):
                        batch = df.iloc[i:i+batch_size].copy()
                        
                        # Convert label_binary to integer (0/1) for GDS compatibility
                        batch['label_binary'] = batch['label_binary'].astype(bool).astype(int)
                        
                        # Convert to records - only select needed columns
                        essential_cols = [
                            'src_ip_zeek', 'dest_ip_zeek', 'ts', 'duration',
                            'service', 'dest_port_zeek', 'conn_state',
                            'label_tactic', 'label_technique', 'label_binary'
                        ]
                        records = []
                        for row in batch[essential_cols].to_dict('records'):
                            row['src_subnet'] = self._ipv4_to_subnet(row['src_ip_zeek'])
                            row['dest_subnet'] = self._ipv4_to_subnet(row['dest_ip_zeek'])
                            records.append(row)

                        # Optimized query with MERGE for IPs (avoids duplicates)
                        # Also compute and set a /24 subnet on each IP node so downstream analyses
                        # that expect subnet metadata will succeed. Use coalesce to preserve existing values.
                        query = """
                        UNWIND $records AS row
                        MERGE (orig:IP {address: row.src_ip_zeek})
                        MERGE (resp:IP {address: row.dest_ip_zeek})
                        // apply subnet metadata only when available from preprocessing
                        SET orig.subnet = coalesce(orig.subnet, row.src_subnet)
                        SET resp.subnet = coalesce(resp.subnet, row.dest_subnet)
                        CREATE (orig)-[:CONNECTS {
                            timestamp: row.ts,
                            duration: row.duration,
                            service: row.service,
                            port: row.dest_port_zeek,
                            state: row.conn_state,
                            tactic: row.label_tactic,
                            technique: row.label_technique,
                            is_attack: row.label_binary
                        }]->(resp)
                        """
                        
                        # This call is now guaranteed to be on an open session
                        try:
                            session.run(query, records=records)
                        except Exception as e:
                            print(f"\nError in batch starting at row {i}: {e}")
                            # Optionally, break or raise here
                            # break 
                        
                        pbar.update(len(batch))

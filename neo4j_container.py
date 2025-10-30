import os
import time
import subprocess
import getpass
from typing import Optional
import socket
import shutil
from neo4j import GraphDatabase, exceptions

class Neo4jContainer:
    """Manages a Neo4j container for the thesis project."""
    
    def __init__(self, container_name: str = "neo4j_thesis_server", 
                 neo4j_image: str = "neo4j:latest",
                 neo4j_user: str = "neo4j",
                 bolt_port: int = 7687,
                 heap_initial: str = "2G",
                 heap_max: str = "4G"):
        """
        Initialize Neo4j container manager.
        
        Args:
            container_name: Name for the container
            neo4j_image: Docker image to use
            neo4j_user: Neo4j username
            bolt_port: Port for Bolt protocol
            heap_initial: Initial heap size
            heap_max: Maximum heap size
        """
        self.container_name = container_name
        self.neo4j_image = neo4j_image
        self.neo4j_user = neo4j_user
        self.bolt_port = bolt_port
        self.heap_initial = heap_initial
        self.heap_max = heap_max
        self.http_port = None
        self.password = None
        self.driver = None

    def _check_port_available(self, port: int) -> bool:
        """Check if a port is available."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return True
            except socket.error:
                return False

    def _get_available_http_port(self) -> Optional[int]:
        """Get an available HTTP port (tries 7474 then 8080)."""
        if self._check_port_available(7474):
            print("Browser port 7474 is available.")
            return 7474
        print("Browser port 7474 is IN USE.")
        if self._check_port_available(8080):
            print("Switching to alternative port 8080.")
            return 8080
        print("FATAL ERROR: Both ports 7474 and 8080 are in use.")
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

    def _ensure_docker_available(self) -> None:
        """Ensure Docker CLI is available and responding (uses `docker image ls`).

        Raises RuntimeError when Docker is missing or the daemon is not responding.
        """
        # Try the simple call first (works when 'docker' is on PATH)
        try:
            proc = subprocess.run(["docker", "image", "ls"], capture_output=True)
            if proc.returncode == 0:
                self._docker_path = shutil.which("docker") or "docker"
                return
        except FileNotFoundError:
            proc = None

        # If the simple call failed, try locating docker via shutil.which
        docker_path = shutil.which("docker")
        if docker_path:
            # try running with absolute path
            try:
                proc2 = subprocess.run([docker_path, "image", "ls"], capture_output=True)
                if proc2.returncode == 0:
                    self._docker_path = docker_path
                    return
                else:
                    stderr = proc2.stderr.decode().strip() if proc2.stderr else ""
                    raise RuntimeError(f"Docker appears to be installed but not usable: {stderr}")
            except FileNotFoundError:
                pass

        # Try common locations (Homebrew on Intel/M1 and typical paths)
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
                        return
                except FileNotFoundError:
                    continue

        # Nothing worked
        raise RuntimeError("Docker CLI not found or not usable. Please install Docker or ensure 'docker' is on PATH and the daemon is running.")

    def _docker_run(self, args, **kwargs):
        """Run a docker command with resolved executable path."""
        if not getattr(self, "_docker_path", None):
            self._ensure_docker_available()
        cmd = [self._docker_path] + args
        return subprocess.run(cmd, **kwargs)

    def _docker_check_output(self, args, **kwargs):
        """Run docker command and return output (like check_output)."""
        if not getattr(self, "_docker_path", None):
            self._ensure_docker_available()
        cmd = [self._docker_path] + args
        return subprocess.check_output(cmd, **kwargs)

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

    def _verify_connection(self) -> bool:
        """Verify connection to Neo4j."""
        try:
            driver = GraphDatabase.driver(
                f"bolt://localhost:{self.bolt_port}",
                auth=(self.neo4j_user, self.password)
            )
            driver.verify_connectivity()
            print('CONNECTION TEST SUCCESSFUL: Python authenticated via Bolt.')
            driver.close()
            return True
        except Exception as e:
            print(f'CONNECTION TEST FAILED: {e}')
            return False

    def start(self, password: Optional[str] = None) -> bool:
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
                    f"Enter the desired password for the '{self.neo4j_user}' user (min 8 chars): "
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
                "-e", f"NEO4J_AUTH={self.neo4j_user}/{self.password}",
                "-e", 'NEO4J_PLUGINS=["graph-data-science"]',
                "--env", f"NEO4J_dbms_memory_heap_initial__size={self.heap_initial}",
                "--env", f"NEO4J_dbms_memory_heap_max__size={self.heap_max}",
                self.neo4j_image
            ]
            self._docker_run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"FATAL ERROR: Docker failed to start the Neo4j container: {e}")
            return False

        print("Waiting 10 seconds for Neo4j to initialize...")
        time.sleep(10)

        # Verify connection
        if not self._verify_connection():
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

if __name__ == "__main__":
    # Example usage
    container = Neo4jContainer()
    container.start()  # Will prompt for password
    
    # To stop and remove:
    # container.stop()
    # container.remove()
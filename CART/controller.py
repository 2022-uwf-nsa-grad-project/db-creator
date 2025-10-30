from .analyzers import *
from .reporting import *
from .base import Neo4jConnection

class Controller(Neo4jConnection):
    """
    A single controller to manage analysis configuration and provide
    access to various analyzer tools.

    This controller serves as the central configuration point for all Neo4j
    connections and container management. It ensures consistent settings
    across all analyzer instances.
    """

    DEFAULT_CONFIG = {
        'uri': "bolt://localhost:7687",
        'user': "neo4j",
        'password': "ubuntuubuntu",
        'database': "neo4j",
        'bolt_port': 7687,
        'neo4j_image': "neo4j:latest",
        'heap_initial': "2G",
        'heap_max': "4G"
    }

    def __init__(self, **config):
        # Merge provided config with defaults
        self.config = self.DEFAULT_CONFIG.copy()
        self.config.update(config)

        # Base name for containers
        self.base_container_name = self.config.get('container_name', 'neo4j_thesis_server')

        # Initialize controller itself as the shared Neo4jConnection
        shared_name = f"{self.base_container_name}"
        super().__init__(
            uri=self.config['uri'],
            user=self.config['user'],
            password=self.config['password'],
            database=self.config['database'],
            container_name=shared_name,
            neo4j_image=self.config.get('neo4j_image', 'neo4j:latest'),
            bolt_port=self.config.get('bolt_port', 7687),
            heap_initial=self.config.get('heap_initial', '2G'),
            heap_max=self.config.get('heap_max', '4G')
        )

        print("Analysis Controller (inherits Neo4jConnection) is configured and ready.")

        # Create analyzer instances (they will reuse the controller instance)
        self.TemporalWindowAnalyzer = self.create_thesis_analyzer()
        self.SubnetPivotAnalyzer = self.create_structural_analyzer()

        # Map suffix -> instance for easier lookup/control
        self.analyzers = {
            'thesis': self.TemporalWindowAnalyzer,
            'structural': self.SubnetPivotAnalyzer,
        }

    # ------------------------------------------------------------------
    # Controller container-management convenience methods
    # These delegate to the underlying analyzer instances so you can
    # control containers directly from the Controller.
    # ------------------------------------------------------------------
    def _get_analyzer(self, key):
        """Resolve an analyzer by key.

        key may be one of:
          - 'thesis' | 'killchain' | 'structural'
          - class attribute name like 'ThesisAnalyzer'
          - the analyzer instance itself
        Returns the analyzer instance or raises KeyError.
        """
        if key is None or key == 'all':
            return None
        if isinstance(key, str):
            k = key.lower()
            if k in self.analyzers:
                return self.analyzers[k]
            # allow class-attribute names
            attr = getattr(self, key, None)
            if attr is not None:
                return attr
            raise KeyError(f"Unknown analyzer key: {key}")
        # assume it's already an instance
        if key in self.analyzers.values():
            return key
        raise KeyError("Unknown analyzer reference")

    def start(self, target='all', password: Optional[str] = None):
        """Start container(s).

        target: 'all' or one of 'thesis','killchain','structural' or the analyzer instance
        password: optional password to pass to start(); if None the analyzer will prompt
        """
        # Compatibility: allow calling start(password) (positional) or start(target=..., password=...)
        # If caller passed a single positional string and didn't set password, treat it as the password
        if password is None and isinstance(target, str) and target.lower() not in ('all', 'thesis', 'killchain', 'structural'):
            # caller used start(password)
            password = target
            target = 'all'

        # prefer controller-level password if not explicitly provided
        pwd = password if password is not None else self.config.get('password')

        # If controller manages a shared connection (via inheritance), operate on it once
        if target == 'all':
            # start controller's connection once
            result = super().start(pwd)
            return {'shared': result}

        ana = self._get_analyzer(target)
        # If analyzer uses the shared connection, delegate to the controller (self)
        if getattr(ana, '_shared_connection', None):
            return super().start(pwd)
        return ana.start(pwd)

    def stop(self, target='all'):
        """Stop container(s)."""
        if target == 'all':
            result = super().stop()
            return {'shared': result}
        ana = self._get_analyzer(target)
        if getattr(ana, '_shared_connection', None):
            return super().stop()
        return ana.stop()

    def remove(self, target='all'):
        """Remove container(s)."""
        if target == 'all':
            result = super().remove()
            return {'shared': result}
        ana = self._get_analyzer(target)
        if getattr(ana, '_shared_connection', None):
            return super().remove()
        return ana.remove()

    def restart(self, target='all'):
        """Restart container(s)."""
        if target == 'all':
            result = super().restart()
            return {'shared': result}
        ana = self._get_analyzer(target)
        if getattr(ana, '_shared_connection', None):
            return super().restart()
        return ana.restart()

    def list_containers(self):
        """Return a dict with each analyzer's container name and running state."""
        status = {}
        for name, ana in self.analyzers.items():
            try:
                running = ana._is_container_running()
            except Exception:
                running = None
            status[name] = {
                'container_name': ana.container_name,
                'running': running,
            }
        return status


    def _get_analyzer_config(self, suffix):
        """Creates a config for a specific analyzer instance."""
        config = self.config.copy()
        config['container_name'] = f"{self.base_container_name}_{suffix}"
        return config

    def create_thesis_analyzer(self):
        """Creates a pre-configured instance of ThesisAnalyzer."""
        return TemporalWindowAnalyzer(connection=self)
        
    def create_structural_analyzer(self):
        """Creates a pre-configured instance of StructuralPivotAnalyzer."""
        return SubnetPivotAnalyzer(connection=self)
    
    @staticmethod
    def create_report_generator(filepath):
        """Creates a pre-configured instance of ReportGenerator."""
        return ReportGenerator(filepath)


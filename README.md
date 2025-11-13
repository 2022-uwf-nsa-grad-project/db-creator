# Project Setup

## Installing Required Packages

To install all the required Python packages for this project, simply run the following command:

```bash
pip install -r requirements.txt
```

This will ensure that all dependencies listed in the `requirements.txt` file are installed in your environment.

## Architecture Overview

This project analyzes APT lateral movement patterns using a hybrid architecture that combines Neo4j graph database capabilities with Polars dataframe processing:

- **Neo4j**: Stores network telemetry as a graph (IP nodes, CONNECTS edges) and generates FastRP embeddings via the Graph Data Science library
- **Polars**: Handles memory-intensive multi-hop chain construction through lazy evaluation and streaming, eliminating the need for arbitrary sampling limits
- **Python Pipeline**: Orchestrates data flow between Neo4j and Polars, performs pivot detection, and generates visualizations

This design offloads computationally expensive operations from Neo4j to Polars, enabling analysis of the complete dataset without memory constraints or query timeouts.

## Neo4jConnection Class Documentation

The `Neo4jConnection` class is a foundational component for managing Neo4j database connections and container lifecycles. Below is an overview of its key features, methods, and best practices.

### Key Features
- **Container Management**: Handles starting, stopping, and restarting Neo4j containers.
- **Database Connection**: Establishes and manages connections to the Neo4j database.
- **Data Ingestion**: Optimized methods for writing large datasets to the database.
- **Configuration**: Supports customizable settings such as heap size, ports, and container names.

### Constructor
```python
Neo4jConnection(
    uri: Optional[str] = None,
    user: str = "neo4j",
    password: Optional[str] = None,
    database: str = "neo4j",
    container_name: str = "neo4j",
    neo4j_image: str = "neo4j:latest",
    bolt_port: int = 7687,
    heap_initial: str = "2G",
    heap_max: str = "4G"
)
```
#### Parameters:
- `uri`: The Bolt URI for the Neo4j instance (default: `bolt://localhost:7687`).
- `user`: Username for the Neo4j instance (default: `neo4j`).
- `password`: Password for the Neo4j instance.
- `database`: Database name (default: `neo4j`).
- `container_name`: Name for the Neo4j container (default: `neo4j`).
- `neo4j_image`: Docker image to use (default: `neo4j:latest`).
- `bolt_port`: Port for the Bolt protocol (default: `7687`).
- `heap_initial`: Initial heap size (default: `2G`).
- `heap_max`: Maximum heap size (default: `4G`).

### Methods

#### `connect()`
Establishes a connection to the Neo4j database.
- **Returns**: `True` if the connection is successful, `False` otherwise.
- **Best Practice**: Ensure the container is running before calling this method.

#### `close()`
Closes the connection to the Neo4j database.
- **Best Practice**: Always call this method to release resources when the connection is no longer needed.

#### `start(password: Optional[str] = "ubuntuubuntu") -> bool`
Starts the Neo4j container.
- **Parameters**: 
  - `password`: Password for the Neo4j user (default: `ubuntuubuntu`).
- **Returns**: `True` if the container starts successfully, `False` otherwise.
- **Best Practice**: Ensure Docker is installed and accessible.

#### `stop() -> bool`
Stops the Neo4j container.
- **Returns**: `True` if the container stops successfully, `False` otherwise.

#### `restart() -> bool`
Restarts the Neo4j container.
- **Returns**: `True` if the container restarts successfully, `False` otherwise.

#### `status() -> dict`
Provides a status summary of the connection and container.
- **Returns**: A dictionary containing details such as container name, Neo4j image, and connection status.

#### `build_database(rebuild=True)`
Orchestrates the database construction process.
- **Parameters**:
  - `rebuild`: If `True`, wipes the existing database before loading new data (default: `True`).
- **Best Practice**: Use this method to ensure the database is properly initialized.

### Best Practices

For more details, refer to the source code in `CART/base.py`.
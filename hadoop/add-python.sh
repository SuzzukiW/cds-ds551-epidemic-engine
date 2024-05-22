#!/bin/bash

# Names of containers to be processed
containers=(
    "namenode"
    "datanode"
    "resourcemanager"
    "nodemanager"
    "historyserver"
)

# Function to process each container
update_and_install() {
    container_name=$1
    echo "Processing container: $container_name"

    # Copy sources.list to container
    podman cp sources.list $container_name:/etc/apt/sources.list

    # Update and install Python
    podman exec -it $container_name bash -c "apt update || :"
    podman exec -it $container_name bash -c "apt install python3 python3-pip -y"

}

# Iterate over containers and apply updates
for container in "${containers[@]}"; do
    update_and_install $container
done

echo "All containers processed."
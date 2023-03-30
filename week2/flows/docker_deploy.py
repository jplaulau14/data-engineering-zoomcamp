from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import etl_parent_flow

docker_container_block = DockerContainer.load("week2-prefect-zoomcamp")

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='week2-docker-flow',
    infrastructure=docker_container_block
)

if __name__ == "__main__":
    deployment.apply()
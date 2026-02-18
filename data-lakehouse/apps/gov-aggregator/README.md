# gov-aggregator (Kafka Streams + Apicurio Avro)

## Local build

```bash
cd ~/Lake/mvp/data-lakehouse/apps/gov-aggregator
mvn -DskipTests package
docker build -f Dockerfile -t mvp-gov-aggregator:local .
```

The compose services use the image configured by `GOV_AGGREGATOR_IMAGE`.

## Jenkins + Harbor deployment

This app now includes a Jenkins pipeline at `Jenkinsfile` that:

1. Builds a Docker image.
2. Pushes it to Harbor.
3. Optionally redeploys `gov-aggregator` and `debezium-to-silver` via Docker Compose.

### Jenkins job setup

- Job type: Pipeline from SCM
- Script path: `data-lakehouse/apps/gov-aggregator/Jenkinsfile`
- Required Jenkins credential:
	- ID: `harbor-credentials`
	- Type: Username with password

### Important parameters

- `HARBOR_REGISTRY` (example: `harbor.local`)
- `HARBOR_PROJECT` (example: `lakehouse`)
- `IMAGE_NAME` (default: `gov-aggregator`)
- `IMAGE_TAG` (example: `2026.02.18`)
- `DEPLOY_WITH_COMPOSE` (`true` to update running services from Jenkins node)

### Compose image wiring

Set the runtime image in [data-lakehouse-ops/.env](../../data-lakehouse-ops/.env):

```dotenv
GOV_AGGREGATOR_IMAGE=harbor.company.local/lakehouse/gov-aggregator:2026.02.18
```

Or pass it inline for one run:

```bash
GOV_AGGREGATOR_IMAGE=harbor.company.local/lakehouse/gov-aggregator:2026.02.18 docker compose up -d gov-aggregator debezium-to-silver
```

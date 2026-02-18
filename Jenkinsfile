pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
  }

  parameters {
    string(name: 'HARBOR_REGISTRY', defaultValue: 'ci-server', description: 'Harbor registry host reachable by Docker')
    string(name: 'HARBOR_PROJECT', defaultValue: 'ndlh', description: 'Harbor project/repository namespace')
    string(name: 'IMAGE_NAME', defaultValue: 'gov-aggregator', description: 'Docker image name')
    string(name: 'IMAGE_TAG', defaultValue: '', description: 'Optional image tag. If empty, Jenkins uses git short SHA')
    string(name: 'DOCKERFILE', defaultValue: 'Dockerfile', description: 'Dockerfile path relative to data-lakehouse/apps/gov-aggregator')
    booleanParam(name: 'PUSH_LATEST_ON_MAIN', defaultValue: true, description: 'Also push :latest when building main/master branch')
    booleanParam(name: 'DEPLOY_WITH_COMPOSE', defaultValue: false, description: 'If true, redeploy gov-aggregator and debezium-to-silver with docker compose on this Jenkins node')
  }

  environment {
    APP_DIR = 'data-lakehouse/apps/gov-aggregator'
    OPS_DIR = 'data-lakehouse-ops'
    HARBOR_CREDENTIALS_ID = 'harbor-credentials'
  }

  stages {
    stage('Preflight') {
      steps {
        sh '''
          set -euo pipefail
          if ! command -v docker >/dev/null 2>&1; then
            echo "ERROR: docker CLI not found inside Jenkins runtime."
            echo "Install docker CLI in Jenkins image or mount it into the container."
            exit 1
          fi

          if [ ! -S /var/run/docker.sock ]; then
            echo "ERROR: /var/run/docker.sock is not mounted into Jenkins."
            echo "Mount Docker socket for same-host build/deploy."
            exit 1
          fi

          docker version
          docker compose version
        '''
      }
    }

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Resolve metadata') {
      steps {
        script {
          env.GIT_SHA = sh(returnStdout: true, script: 'git rev-parse --short=12 HEAD').trim()
          env.RESOLVED_TAG = params.IMAGE_TAG?.trim() ? params.IMAGE_TAG.trim() : env.GIT_SHA
          env.IMAGE_REF = "${params.HARBOR_REGISTRY}/${params.HARBOR_PROJECT}/${params.IMAGE_NAME}"
          env.DEPLOY_IMAGE = "${env.IMAGE_REF}:${env.RESOLVED_TAG}"
        }
        echo "IMAGE_REF=${env.IMAGE_REF}"
        echo "RESOLVED_TAG=${env.RESOLVED_TAG}"
        echo "DEPLOY_IMAGE=${env.DEPLOY_IMAGE}"
      }
    }

    stage('Build image') {
      steps {
        dir("${env.APP_DIR}") {
          sh '''
            set -euo pipefail
            docker build \
              -f "${DOCKERFILE}" \
              -t "${IMAGE_REF}:${RESOLVED_TAG}" \
              -t "${IMAGE_REF}:${BUILD_NUMBER}" \
              -t "${IMAGE_REF}:${GIT_SHA}" \
              .
          '''
        }
      }
    }

    stage('Push image to Harbor') {
      steps {
        withCredentials([usernamePassword(credentialsId: env.HARBOR_CREDENTIALS_ID, usernameVariable: 'HARBOR_USERNAME', passwordVariable: 'HARBOR_PASSWORD')]) {
          sh '''
            set -euo pipefail
            echo "${HARBOR_PASSWORD}" | docker login "${HARBOR_REGISTRY}" --username "${HARBOR_USERNAME}" --password-stdin
            docker push "${IMAGE_REF}:${RESOLVED_TAG}"
            docker push "${IMAGE_REF}:${BUILD_NUMBER}"
            docker push "${IMAGE_REF}:${GIT_SHA}"

            if [[ "${PUSH_LATEST_ON_MAIN}" == "true" && ( "${BRANCH_NAME:-}" == "main" || "${BRANCH_NAME:-}" == "master" ) ]]; then
              docker tag "${IMAGE_REF}:${RESOLVED_TAG}" "${IMAGE_REF}:latest"
              docker push "${IMAGE_REF}:latest"
            fi

            docker logout "${HARBOR_REGISTRY}" || true
          '''
        }
      }
    }

    stage('Deploy with docker compose') {
      when {
        expression { params.DEPLOY_WITH_COMPOSE }
      }
      steps {
        dir("${env.OPS_DIR}") {
          sh '''
            set -euo pipefail
            GOV_AGGREGATOR_IMAGE="${DEPLOY_IMAGE}" docker compose pull gov-aggregator debezium-to-silver
            GOV_AGGREGATOR_IMAGE="${DEPLOY_IMAGE}" docker compose up -d gov-aggregator debezium-to-silver
          '''
        }
      }
    }
  }

}

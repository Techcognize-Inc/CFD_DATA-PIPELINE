pipeline {
    agent any

    environment {
        COMPOSE_FILE = 'docker-compose.yml'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Validate Python') {
            steps {
                sh '''
                python3 -m py_compile producer/csv_replayer.py
                python3 -m py_compile spark/stream_features.py
                python3 -m py_compile spark/batch_rule_engine.py
                python3 -m py_compile airflow/dags/fraud_rule_engine_dag.py
                python3 -m py_compile airflow/dags/fraud_streaming_monitor_dag.py
                '''
            }
        }

        stage('Validate Docker Compose') {
            steps {
                sh '''
                docker compose -f ${COMPOSE_FILE} config
                '''
            }
        }

        stage('Start Infra') {
            steps {
                sh '''
                docker compose -f ${COMPOSE_FILE} up -d
                '''
            }
        }

        stage('Wait for Services') {
            steps {
                sh 'sleep 30'
            }
        }

        stage('Initialize Database') {
            steps {
                sh '''
                docker exec de2-postgres psql -U banking -d bankingdb -f /sql/init.sql
                '''
            }
        }

        stage('Smoke Check') {
            steps {
                sh '''
                docker exec de2-postgres psql -U banking -d bankingdb -c "SELECT 1;"
                '''
            }
        }
    }

    post {
        success {
            echo 'Pipeline SUCCESS'
        }
        failure {
            echo 'Pipeline FAILED'
        }
    }
}
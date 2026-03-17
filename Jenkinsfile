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
                bat '''
                python -m py_compile producer\\csv_replayer.py
                python -m py_compile spark\\stream_features.py
                python -m py_compile spark\\batch_rule_engine.py
                '''
            }
        }

        stage('Validate Docker Compose') {
            steps {
                bat '''
                docker compose -f %COMPOSE_FILE% config
                '''
            }
        }

        stage('Start Infra') {
            steps {
                bat '''
                docker compose -f %COMPOSE_FILE% up -d
                '''
            }
        }

        stage('Wait for Services') {
            steps {
                bat 'timeout /t 30'
            }
        }

        stage('Initialize Database') {
            steps {
                bat '''
                docker exec de2-postgres psql -U banking -d bankingdb -f /sql/init.sql
                '''
            }
        }

        stage('Smoke Check') {
            steps {
                bat '''
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
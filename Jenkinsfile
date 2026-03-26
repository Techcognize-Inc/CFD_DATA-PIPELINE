pipeline {
    agent any

    options {
        timestamps()
    }

    environment {
        PYTHONUNBUFFERED = "1"
    }

    stages {
        stage('Checkout Code') {
            steps {
                checkout scm
            }
        }

        stage('Verify Project Structure') {
            steps {
                bat '''
                if not exist producer\\csv_replayer.py exit /b 1
                if not exist spark\\stream_features.py exit /b 1
                if not exist spark\\batch_rule_engine.py exit /b 1
                if not exist spark\\rule_utils.py exit /b 1
                if not exist sql\\init.sql exit /b 1
                if not exist sql\\generate_alerts.sql exit /b 1
                if not exist requirements-dev.txt exit /b 1
                if not exist tests\\test_producer.py exit /b 1
                if not exist tests\\test_rule_utils.py exit /b 1
                '''
            }
        }

        stage('Create Virtual Environment') {
            steps {
                bat '''
                python -m venv venv
                call venv\\Scripts\\activate
                python -m pip install --upgrade pip
                pip install -r requirements-dev.txt
                '''
            }
        }

        stage('Run Unit Tests') {
            steps {
                bat '''
                call venv\\Scripts\\activate
                pytest tests --junitxml=pytest-results.xml
                '''
            }
        }

        stage('Validate Python Syntax') {
            steps {
                bat '''
                call venv\\Scripts\\activate
                python -m py_compile producer\\csv_replayer.py
                python -m py_compile spark\\stream_features.py
                python -m py_compile spark\\batch_rule_engine.py
                python -m py_compile spark\\rule_utils.py
                '''
            }
        }

        stage('Validate Docker Compose') {
            steps {
                bat '''
                docker compose config
                '''
            }
        }
    }

    post {
        always {
            junit allowEmptyResults: true, testResults: 'pytest-results.xml'
        }
        success {
            echo 'Jenkins pipeline completed successfully.'
        }
        failure {
            echo 'Jenkins pipeline failed. Check the logs.'
        }
    }
}
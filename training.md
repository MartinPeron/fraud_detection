flowchart TD

    K[Test Metrics]
    %% --- LOCAL ENVIRONMENT ---
    subgraph L[Training Pipeline]
        A[Load Data Source: Fraud Transactions CSV]
        B[Feature Creation and Engineering]
        C1[Model Training]
        C2[Model Prediction]
        A --> B --> C1 --> C2
    end
    C2 --> K

    %% --- FEATURES + MODEL BUNDLE ---
    H[Features + Model]
    B --> H
    C1 --> H

    %% --- MLFLOW INFRASTRUCTURE ---
    subgraph M[MLflow Tracking Infrastructure]
        D[MLflow Tracking Server]
        E[Experiment Management and Model Registry]
        D --> E
    end

    %% --- CLOUD INFRASTRUCTURE ---
    subgraph Cld[Cloud Infrastructure]
        subgraph S[AWS S3]
            F[Artifact Repository and Model Storage]
        end
        subgraph P[PostgreSQL Database]
            G[Metrics and Metadata Storage]
        end
    end

    %% --- DASHBOARD ---
    J[Metrics Visualization Dashboard]

    %% --- CONNECTIONS ---
    H --> D
    K --> D
    E --> F
    E --> G
    E --> J

    %% --- STYLING ---
    classDef io fill:#dfe9f3,stroke:#6b7a8f,stroke-width:1px,color:#000;
    classDef process fill:#e7f5f2,stroke:#4f9d69,stroke-width:1px,color:#000;
    classDef infra fill:#fff4e6,stroke:#e69500,stroke-width:1px,color:#000;

    class A,B,C1,C2 process
    class D,E,F,G infra

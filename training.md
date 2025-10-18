flowchart LR

    %% --- LOCAL ENVIRONMENT ---
    subgraph L[Training]
        A[Load Data Source: Fraud Transactions CSV]
        B[Feature Creation and Engineering]
        C1[Model Training]
        C2[Model Prediction]
        A --> B --> C1 --> C2
    end

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

    %% --- CONNECTIONS BETWEEN BLOCKS ---
    %% Training phase connections
    B --> D
    C1 --> D
    D --> F
    D --> G

    %% Prediction phase connections
    C2 --> D
    C2 --> H[Prediction Metrics]
    H --> G

    %% MLflow visualization
    E --> F
    E --> G
    E --> I[Metrics Visualization Dashboard]

    %% --- STYLING ---
    classDef io fill:#dfe9f3,stroke:#6b7a8f,stroke-width:1px,color:#000;
    classDef process fill:#e7f5f2,stroke:#4f9d69,stroke-width:1px,color:#000;
    classDef infra fill:#fff4e6,stroke:#e69500,stroke-width:1px,color:#000;

    class A,B,C1,C2 process
    class D,E infra
    class F,G infra
    class H,I io

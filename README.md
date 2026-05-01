Definir pipeline limpio con separación de responsabilidades razonable
1. Dataset raw ( heterogeneidad y agnosticismo )
	1. Generador de divergencias (test)
2. [[Canonización]]( por inferencia ) -> convertimos a un modelo estandard los tipos - Reglas PRE
	1. Schema inspection (introspection)
	2. Validation Layer - Normalizar, Higienizar
	3. Canonical View
3. Motor de diferencias basado en hashes (**data-diff**) 
4. [[Clasificación]]: Separación (INS|DEL vs UPD) -
5. Normalización UPD
	1. [[IA]] para :
		1. discriminar falsos positivos
		2. generar heurísticas
	2. Aplicación de reglas POST
		1. Márgenes de sensibilidad y tolerancia ( edades , temperatura )
		2. Proximidad en cadenas
		3. Semántica ( Embeddings [^1])
6. Acciones correctivas ( DiffEvents )
7. Reporte

 ==Definición general==: pipeline de dos capas para la comparación de datos heterogéneos entre motores SQL. La primera capa (canonización SQL) reduce falsos positivos estructurales antes de que lleguen al comparador; la segunda capa (clasificación LLM) categoriza semánticamente las diferencias que quedan
 ==Arquitectura funcional==:
 
![[arquitectura funcional.png]]
![[Flujo del pipeline.png]]

 ==[[Patrones de diseño]]== : 

 ==Módulos internos:== 
![[módulos internos.png]]

[^1]:  Para cada diferencia:

``` python 
from sentence_transformers import SentenceTransformer  
import numpy as np
  
model = SentenceTransformer("all-MiniLM-L6-v2")  
  
def semantic_similarity(a, b):
    emb = model.encode([str(a), str(b)])  
    return np.dot(emb[0], emb[1]) / ( 
        np.linalg.norm(emb[0]) * np.linalg.norm(emb[1])
    )
```

Heurística típica:
- > 0.9 → no semántico
- 0.7–0.9 → dudoso
- < 0.7 → cambio semántico probable

 ```mermaid
flowchart TD
    A["🗂️ Dataset Raw - Heterogéneo"]:::input

    subgraph CANON["📐 Canonización — Reglas PRE"]
      SI["Schema Inspection\n(introspection)"]:::canon
      VL["Validation & Hygiene Layer\n(normalizar · higienizar)"]:::canon
      CV["Canonical View"]:::canon
    end

    subgraph DIFF["⚙️ Motor de Diferencias"]
      HD["Hash-based Data-Diff"]:::diff
      CL["Clasificación\nINS · DEL · UPD"]:::diff
    end

    subgraph UPD["🤖 Normalización UPD"]
      AI["IA: discriminar FP\ngenerar heurísticas"]:::ai
      POST["Reglas POST + Confidence Score"]:::ai
      SEN["Márgenes de sensibilidad\n(edad · temperatura)"]:::rule
      LEV["Proximidad léxica\n(edit distance)"]:::rule
      EMB["Semántica — Embeddings"]:::rule
    end

    subgraph OUT["📤 Salida"]
      DE["DiffEvents\n(con score de confianza)"]:::output
      RP["Reporte"]:::output
    end

    SV["📋 Schema Versioning"]:::new
    FB["🔄 Feedback Loop\n(validación → heurísticas)"]:::new

    A --> CANON
    GEN -.->|"divergencias sintéticas"| CANON
    SI --> VL
    VL --> SV
    SV --> CV
    HD --> CL
    CL -->|"INS / DEL"| DE
    CL -->|"UPD candidatos"| AI
    AI --> POST
    POST --> SEN
    POST --> LEV
    POST --> EMB
    SEN --> DE
    LEV --> DE
    EMB --> DE
    DE --> RP
    DE -.->|"validación"| FB
    FB -.->|"actualiza reglas"| AI


    classDef input  fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef test   fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef canon  fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef diff   fill:#FAEEDA,stroke:#854F0B,color:#412402
    classDef ai     fill:#FBEAF0,stroke:#993556,color:#4B1528
    classDef rule   fill:#FAECE7,stroke:#993C1D,color:#4A1B0C
    classDef output fill:#EAF3DE,stroke:#3B6D11,color:#173404
    classDef new    fill:#E1F5EE,stroke:#1D9E75,color:#04342C,stroke-dasharray:4 3
    classDef cross  fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A,stroke-dasharray:3 3

```

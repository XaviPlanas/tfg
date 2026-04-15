CLASSIFY_PROMPT = """
Eres un experto en bases de datos heterogéneas. Estás analizando 
diferencias detectadas por data-diff entre {source_a} y {source_b}.

Contexto del esquema:
{schema_context}

Fila en {source_a}:
{row_a}

Fila en {source_b}:
{row_b}

Clasifica la diferencia en UNA de estas categorías:

- REAL: los datos son semánticamente distintos. La diferencia 
  es significativa e indica una divergencia real entre los sistemas.

- FALSO_POSITIVO_TIPO: mismo valor, representación distinta por 
  diferencias en tipos de datos entre motores (coma flotante, 
  precisión numérica, booleanos, timestamps).

- FALSO_POSITIVO_NORMALIZACION: mismo valor semántico, representación 
  distinta por normalización asimétrica (encoding, collation, 
  mayúsculas, espacios, charset).

- AMBIGUA: no se puede determinar la categoría con los datos 
  disponibles.

Responde ÚNICAMENTE con JSON válido, sin texto adicional:
{{
    "categoria": "REAL|FALSO_POSITIVO_TIPO|FALSO_POSITIVO_NORMALIZACION|AMBIGUA",
    "confianza": <float entre 0.0 y 1.0>,
    "columnas_afectadas": ["col1", "col2"],
    "explicacion": "<explicación concisa en español>",
    "normalizacion_sugerida": "<expresión SQL o null>"
}}
"""


CLASSIFY_PROMPT_FROM_DICT = """
Eres un experto en bases de datos heterogéneas. Estás analizando 
diferencias detectadas por data-diff entre {source_a} y {source_b}.
 Los valores de los datos se proporcionan en formato diccionario de diccionario python
 la primera clave es la clave primaria y el valor es otro diccionario con pares clave valor que
 corresponden a cada columna y su valor

Contexto del esquema:

**Variable**

**Definition**

**Key**

survival

Survival

0 = No, 1 = Yes

pclass

Ticket class

1 = 1st, 2 = 2nd, 3 = 3rd

sex

Sex

Age

Age in years

sibsp

\# of siblings / spouses aboard the Titanic

parch

\# of parents / children aboard the Titanic

ticket

Ticket number

fare

Passenger fare

cabin

Cabin number

embarked

Port of Embarkation

C = Cherbourg, Q = Queenstown, S = Southampton

Fila en {source_a}:
{row_a}

Fila en {source_b}:
{row_b}

Clasifica la diferencia en UNA de estas categorías:

- REAL: los datos son semánticamente distintos. La diferencia 
  es significativa e indica una divergencia real entre los sistemas.

- FALSO_POSITIVO_TIPO: mismo valor, representación distinta por 
  diferencias en tipos de datos entre motores (coma flotante, 
  precisión numérica, booleanos, timestamps).

- FALSO_POSITIVO_NORMALIZACION: mismo valor semántico, representación 
  distinta por normalización asimétrica (encoding, collation, 
  mayúsculas, espacios, charset).

- AMBIGUA: no se puede determinar la categoría con los datos 
  disponibles.

Responde ÚNICAMENTE con JSON válido, sin texto adicional:
{{
    "categoria": "REAL|FALSO_POSITIVO_TIPO|FALSO_POSITIVO_NORMALIZACION|AMBIGUA",
    "confianza": <float entre 0.0 y 1.0>,
    "columnas_afectadas": ["col1", "col2"],
    "explicacion": "<explicación concisa en español>",
    "normalizacion_sugerida": "<expresión SQL o null>"
}}
"""

CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_1 = """
Assume the role of an expert in data integration and schema alignment, with deep understanding 
of data inconsistencies across heterogeneous systems. Your task is to help me evaluate a difference
 between two records (row_a and row_b), provided as dictionary structures keyed by primary key. 
 Using the schema definitions (including encoded categorical values like embarked, survival, etc.), 
 determine if the discrepancy represents a real divergence or a false positive caused by type 
 differences or normalization inconsistencies. Perform a structured comparison column by column, 
 identify affected fields, and infer semantic equivalence where possible. Output must be strictly 
 valid JSON with fields: categoria, confianza (0–1), columnas_afectadas, explicacion (concise Spanish),
   and normalizacion_sugerida (SQL or null). One output per primary key. Ensure no extra text outside the JSON.
 ###  This is the data schema:
   | Columna     | Tipo      | Descripción                                      | Valores posibles / Notas                  |
|-------------|-----------|--------------------------------------------------|-------------------------------------------|
| survival    | int       | Sobrevivió o no                                  | 0 = No, 1 = Sí (target)                  |
| pclass      | int       | Clase del ticket                                 | 1 = 1ª, 2 = 2ª, 3 = 3ª                    |
| sex         | string    | Sexo                                             | male / female                             |
| age         | float     | Edad en años                                     | Puede tener decimales y valores nulos     |
| sibsp       | int       | Hermanos/esposos a bordo                         | Numérico                                  |
| parch       | int       | Padres/hijos a bordo                             | Numérico                                  |
| ticket      | string    | Número de ticket                                 | Alfanumérico                              |
| fare        | float     | Tarifa pagada                                    | Numérico                                  |
| cabin       | string    | Número de camarote                               | Muchos valores nulos (~77%)               |
| embarked    | string    | Puerto de embarque                               | C=Cherbourg, Q=Queenstown, S=Southampton (2 nulos) |

 ###  These are the data rows to compare by primary keys
   for {source_a} the data is {row_a}
   for  {source_b} the data is {row_b} 
  
"""

CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_2="""
You are an expert in data reconciliation across heterogeneous systems.

<<DATA_STRUCTURE>>

* **Key:**  Each line in the dataset corresponds to a single passenger on the Titanic. For each passenger, there are keys associated with different attributes (e.g., PassengerId). 
* **Passenger Details:** Each line holds details about a specific passenger including their:
    - **Survival Status:** Whether the passenger survived or not (`Survived`)
    - **Pclass:**  Their class on the ship (1st, 2nd, or 3rd)
    - **Name:**  The passenger's full name.
    - **Sex:** The passenger's gender 
    - **Age:**  The passenger's age at time of travel.   
    - **SibSp:** Number of siblings/spouses traveling with them. 
    - **Parch:** Number of parents/children traveling with them.
    - **Ticket:** Their ticket number or the unique identifier for each individual.
    - **Fare:** The cost of their ticket.
    - **Cabin:** The cabin number they were assigned on the ship (if applicable). 
<<DATA_STRUCTURE>>

I will provide two records DATASET_A from {source_a} and DATASET_B from {source_b}) with the same primary key and a shared schema.
The primary key is the first key of the dictionary of dictionaries.

Task:

1) Compare the two datasets , row by row, using the primary keys.
2) Detect:
- Real differences → "diferente"
- Formatting/type issues → "equivalente"
- Normalization artifacts  → "inconsistencia_normalizacion"
- Infer semantic equivalence when possible

Return ONLY valid JSON:
{{
"categoria": "...",
"confianza": 0.0,
"columnas_afectadas": ["..."],
"explicacion": "explicación breve en español",
"normalizacion_sugerida": "SQL o null"
}}

Constraints:
- No extra text
- Create multiple JSON: One JSON per each primary key from the two datasets 
- Be conservative before labeling as "diferente"

IMPORTANT:
- No text outside JSON
- No markdown
- No questions
- No metadata
- No schema description
- You MUST process ALL keys
- If dataset is large, continue until end without stopping

<< DATA >>
<< DATASET_A {source_a} >>
 {row_a} 
<</DATASET_A>>

<<RDATASET_B {source_b}>>
{row_b}
<</DATASET_B>>
<< /DATA >>
"""
CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_GEMMA2_2b= """You are an expert in data reconciliation. 

### SCHEMA (Titanic)
- Primary key: PassengerId (first key in each dict)
- Columns: survival, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked

I will give you two datasets with identical schema.

Task:
1. Compare row-by-row using PassengerId as key.
2. For each PassengerId decide ONLY:
   - "diferente" → real difference
   - "equivalente" → same value or only formatting/type
   - "inconsistencia_normalizacion" → normalization artifact

Return EXACTLY this and NOTHING ELSE:
[
  {{"categoria": "diferente|equivalente|inconsistencia_normalizacion", "confianza": 0.XX, "columnas_afectadas": ["col1","col2"], "explicacion": "breve en español", "normalizacion_sugerida": "SQL o null"}},
  ...
]

No markdown, no texto extra, no explicación, no ```json. Solo el array JSON.

<<DATASET_A {source_a}>>
{row_a}
<</DATASET_A>>

<<DATASET_B {source_b}>>
{row_b}
<</DATASET_B>>
"""
import json

#from ollama import Client
from tfg.datadiff_classifier.models import DiffRow, ClassificationResult, DiffCategory
from tfg.datadiff_classifier.prompts import CLASSIFY_PROMPT, CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_2, CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_GEMMA2_2b

class DiffClassifier:
    
    def __init__(self, schema_context: str):
        self.schema_context = schema_context
        self.client = client = Client(
            host='http://127.0.0.1:11434')
            #host='https://ollama.com',
            #headers={'Authorization': 'Bearer ' +  os.environ.get('OLLAMA_API_KEY')} ) 

    def classify(self, diff_row: DiffRow) -> ClassificationResult:
        
        prompt = CLASSIFY_PROMPT_FROM_DICT_OPTIMIZED_GEMMA2_2b.format(
            source_a        = diff_row.source_a,
            source_b        = diff_row.source_b,
            schema_context  = self.schema_context,
            row_a           = json.dumps(diff_row.row_a, ensure_ascii=False),
            row_b           = json.dumps(diff_row.row_b, ensure_ascii=False),            
            # row_a           = json.dumps(diff_row.row_a, indent=2, ensure_ascii=False),
            # row_b           = json.dumps(diff_row.row_b, indent=2, ensure_ascii=False),
        )
        print(f"Utilizando el PROMPT: {prompt}")
        response = self.client.generate(
            #model='gemma2:2b',
            model='qwen3:8b',
            prompt=prompt
            , options={
                 "num_ctx": 8192,
                 "temperature": 0.0,
                 "top_p": 0.1,
                 "top_k": 40,
            #     "num_predict": 100
            }
)
        #print(json.dumps(response.model_dump(), indent=2))
        print(response["response"])
        print("Total (ms):", response['total_duration']/1e6)
        print("Prompt eval (ms):", response['prompt_eval_duration']/1e6)
        print("Generation (ms):", response['eval_duration']/1e6)

        # return ClassificationResult(
        #     key                     = diff_row.key,
        #     categoria               = DiffCategory(raw["categoria"]),
        #     confianza               = raw["confianza"],
        #     columnas_afectadas      = raw["columnas_afectadas"],
        #     explicacion             = raw["explicacion"],
        #     normalizacion_sugerida  = raw.get("normalizacion_sugerida"),
        #     row_a                   = diff_row.row_a,
        #     row_b                   = diff_row.row_b,
        # )

    def classify_batch(
        self, 
        diff_rows: list[DiffRow],
        confidence_threshold: float = 0.85
    ) -> list[ClassificationResult]:
        """
        Clasifica una lista de diferencias. Las clasificaciones
        por debajo del umbral de confianza se marcan como AMBIGUA
        para revisión manual.
        """
        results = []
        for row in diff_rows:
            result = self.classify(row)
            if result.confianza < confidence_threshold:
                result.categoria = DiffCategory.AMBIGUA
            results.append(result)
        return results
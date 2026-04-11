import json
import os
from ollama import Client
from tfg.datadiff_classifier.models import DiffRow, ClassificationResult, DiffCategory, IAModel
from tfg.datadiff_classifier.prompts import CLASSIFY_PROMPT

class DiffClassifier:
    
    def __init__(self, schema_context: str):
        self.schema_context = schema_context
        self.client = client = Client(
            host='http://127.0.0.1:11434')
            #host='https://ollama.com',
            #headers={'Authorization': 'Bearer ' +  os.environ.get('OLLAMA_API_KEY')} ) 

    def classify(self, diff_row: DiffRow) -> ClassificationResult:
        prompt = CLASSIFY_PROMPT.format(
            source_a        = diff_row.source_a,
            source_b        = diff_row.source_b,
            schema_context  = self.schema_context,
            row_a           = json.dumps(diff_row.row_a, indent=2, ensure_ascii=False),
            row_b           = json.dumps(diff_row.row_b, indent=2, ensure_ascii=False),
        )

        response = self.client.generate(
            model       = 'gemma2:2b',
            #max_tokens  = 1000,
            #messages    = [{"role": "user", "content": prompt}]
            prompt       = prompt 
        )

       # raw = json.loads(response.content[0].text)
        # content = response.get('message', {}).get('content', '')
        print(json.dumps(response['content'], indent=2))
        print(json.dumps(response.model_dump(), indent=2))
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
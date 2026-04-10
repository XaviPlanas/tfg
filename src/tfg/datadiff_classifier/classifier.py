import json
import anthropic
from models import DiffRow, ClassificationResult, DiffCategory, IAModel
from prompts import CLASSIFY_PROMPT

class DiffClassifier:
    
    def __init__(self, schema_context: str, model: IAModel = IAModel.ANTHROPIC):
        self.schema_context = schema_context
        self.model = model

        if model == IAModel.ANTHROPIC :
            self.client = anthropic.Anthropic()

    def classify(self, diff_row: DiffRow) -> ClassificationResult:
        prompt = CLASSIFY_PROMPT.format(
            source_a        = diff_row.source_a,
            source_b        = diff_row.source_b,
            schema_context  = self.schema_context,
            row_a           = json.dumps(diff_row.row_a, indent=2, ensure_ascii=False),
            row_b           = json.dumps(diff_row.row_b, indent=2, ensure_ascii=False),
        )

        response = self.client.messages.create(
            model       = self.model,
            max_tokens  = 1000,
            messages    = [{"role": "user", "content": prompt}]
        )

        raw = json.loads(response.content[0].text)

        return ClassificationResult(
            key                     = diff_row.key,
            categoria               = DiffCategory(raw["categoria"]),
            confianza               = raw["confianza"],
            columnas_afectadas      = raw["columnas_afectadas"],
            explicacion             = raw["explicacion"],
            normalizacion_sugerida  = raw.get("normalizacion_sugerida"),
            row_a                   = diff_row.row_a,
            row_b                   = diff_row.row_b,
        )

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
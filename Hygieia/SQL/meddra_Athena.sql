WITH meddra_concepts AS (
    SELECT
        concept_id,
        concept_code,
        concept_name,
        concept_class_id
    FROM omop54.concept
    WHERE vocabulary_id = 'MedDRA'
      AND invalid_reason IS NULL
      AND concept_class_id IN ('SOC', 'HLGT', 'HLT', 'PT', 'LLT')
),

meddra_edges AS (
    SELECT
        concept_id_1 AS parent_concept_id,
        concept_id_2 AS child_concept_id
    FROM omop54.concept_relationship
    WHERE relationship_id = 'Subsumes'
      AND invalid_reason IS NULL
)

SELECT DISTINCT
    soc.concept_id      AS soc_concept_id,
    soc.concept_code    AS soc_code,
    soc.concept_name    AS soc,

    hlgt.concept_id     AS hlgt_concept_id,
    hlgt.concept_code   AS hlgt_code,
    hlgt.concept_name   AS hlgt,

    hlt.concept_id      AS hlt_concept_id,
    hlt.concept_code    AS hlt_code,
    hlt.concept_name    AS hlt,

    pt.concept_id       AS pt_concept_id,
    pt.concept_code     AS pt_code,
    pt.concept_name     AS pt,

    llt.concept_id      AS llt_concept_id,
    llt.concept_code    AS llt_code,
    llt.concept_name    AS llt

FROM meddra_concepts soc
JOIN meddra_edges soc_to_hlgt
  ON soc_to_hlgt.parent_concept_id = soc.concept_id

JOIN meddra_concepts hlgt
  ON hlgt.concept_id = soc_to_hlgt.child_concept_id
 AND hlgt.concept_class_id = 'HLGT'

JOIN meddra_edges hlgt_to_hlt
  ON hlgt_to_hlt.parent_concept_id = hlgt.concept_id

JOIN meddra_concepts hlt
  ON hlt.concept_id = hlgt_to_hlt.child_concept_id
 AND hlt.concept_class_id = 'HLT'

JOIN meddra_edges hlt_to_pt
  ON hlt_to_pt.parent_concept_id = hlt.concept_id

JOIN meddra_concepts pt
  ON pt.concept_id = hlt_to_pt.child_concept_id
 AND pt.concept_class_id = 'PT'

LEFT JOIN meddra_edges pt_to_llt
  ON pt_to_llt.parent_concept_id = pt.concept_id

LEFT JOIN meddra_concepts llt
  ON llt.concept_id = pt_to_llt.child_concept_id
 AND llt.concept_class_id = 'LLT'

WHERE soc.concept_class_id = 'SOC'

ORDER BY
    soc,
    hlgt,
    hlt,
    pt,
    llt;
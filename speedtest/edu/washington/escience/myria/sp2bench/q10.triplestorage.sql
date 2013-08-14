SELECT
    D1.val  AS subject,
    DP1.val AS predicate
FROM
    Triples T    
    JOIN Dictionary DP1 ON T.predicate=DP1.ID
    JOIN Dictionary D1  ON T.subject=D1.ID
    JOIN Dictionary D2  ON T.object=D2.ID
WHERE
    D2.val='person:Paul_Erdoes';

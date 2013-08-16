SELECT 
    D.val AS ee
FROM
    Triples T 
    JOIN Dictionary DP ON T.predicate=DP.ID
    JOIN Dictionary D  ON T.object=D.ID
WHERE
    DP.val='rdfs:seeAlso'
ORDER BY ee
LIMIT 10
OFFSET 50;


SELECT DISTINCT
    D2.val AS person,
    D4.val AS name
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Dictionary D2  ON T2.object=D2.ID
    JOIN Triples T3     ON T2.object=T3.object
    JOIN Dictionary DP3 ON T3.predicate=DP3.ID
    JOIN Triples T4     ON T3.subject=T4.subject
    JOIN Dictionary DP4 ON T4.predicate=DP4.ID
    JOIN Dictionary D3  ON T4.object=D3.ID
    JOIN Triples T5     ON T3.object=T5.subject
    JOIN Dictionary DP5 ON T5.predicate=DP5.ID
    JOIN Dictionary D4  ON T5.object=D4.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:creator'
    AND DP3.val='dc:creator'
    AND DP4.val='rdf:type'
    AND DP5.val='foaf:name'
    AND D1.val='bench:Article'
    AND D3.val='bench:Inproceedings';


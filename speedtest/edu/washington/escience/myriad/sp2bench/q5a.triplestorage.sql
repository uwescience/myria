SELECT DISTINCT
    D2.val AS person,
    D3.val AS name
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID 
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Dictionary D2  ON T2.object=D2.ID 
    JOIN Triples T3     ON T2.object=T3.subject
    JOIN Dictionary DP3 ON T3.predicate=DP3.ID
    JOIN Dictionary D3  ON T3.object=D3.ID,
    Triples T4
    JOIN Dictionary DP4 ON T4.predicate=DP4.ID
    JOIN Dictionary D4  ON T4.object=D4.ID 
    JOIN Triples T5     ON T4.subject=T5.subject
    JOIN Dictionary DP5 ON T5.predicate=DP5.ID
    JOIN Dictionary D5  ON T5.object=D5.ID 
    JOIN Triples T6     ON T5.object=T6.subject
    JOIN Dictionary DP6 ON T6.predicate=DP6.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:creator'
    AND DP3.val='foaf:name'
    AND DP4.val='rdf:type'
    AND DP5.val='dc:creator'
    AND DP6.val='foaf:name'
    AND D1.val='bench:Article'
    AND D4.val='bench:Inproceedings'
    AND T3.object=T6.object;


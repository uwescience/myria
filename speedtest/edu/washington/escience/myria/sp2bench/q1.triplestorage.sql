SELECT
    D3.val AS yr
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Dictionary D2  ON T2.object=D2.ID
    JOIN Triples T3     ON T1.subject=T3.subject
    JOIN Dictionary DP3 ON T3.predicate=DP3.ID
    JOIN Dictionary D3  ON T3.object=D3.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:title'
    AND DP3.val='dcterms:issued'
    AND D1.val='bench:Journal'
    AND D2.val='"Journal 1 (1940)"^^xsd:string';

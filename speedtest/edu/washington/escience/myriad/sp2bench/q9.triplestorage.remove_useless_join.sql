SELECT DISTINCT L.predicate AS predicate
FROM
    (
        SELECT
            DP2.val AS predicate
        FROM 
            Triples T1
            JOIN Dictionary DP1 ON T1.predicate=DP1.ID
            JOIN Dictionary D2  ON T1.object=D2.ID
            JOIN Triples T2     ON T1.subject=T2.object
            JOIN Dictionary DP2 ON  T2.predicate=DP2.ID
        WHERE
            DP1.val='rdf:type'
            AND D2.val='foaf:Person'
        UNION
        SELECT
            DP2.val AS predicate
        FROM 
            Triples T1
            JOIN Dictionary DP1 ON T1.predicate=DP1.ID
            JOIN Dictionary D2  ON T1.object=D2.ID
            JOIN Triples T2     ON T1.subject=T2.subject
            JOIN Dictionary DP2 ON T2.predicate=DP2.ID
        WHERE
            DP1.val='rdf:type'
            AND D2.val='foaf:Person'
    ) L;



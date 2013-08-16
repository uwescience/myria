SELECT
    count(D1.val) AS article
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.subject=D1.ID
    JOIN Dictionary D2  ON T1.object=D2.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='swrc:pages'
    AND D2.val='bench:Article';


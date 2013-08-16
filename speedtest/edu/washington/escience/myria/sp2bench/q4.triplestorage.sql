SELECT DISTINCT
    D2.val AS name1,
    D4.val AS name2
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Triples T3     ON T2.object=T3.subject
    JOIN Dictionary DP3 ON T3.predicate=DP3.ID
    JOIN Dictionary D2  ON T3.object=D2.ID
    JOIN Triples T4     ON T1.subject=T4.subject
    JOIN Dictionary DP4 ON T4.predicate=DP4.ID
    JOIN Triples T5     ON T4.object=T5.object
    JOIN Dictionary DP5 ON T5.predicate=DP5.ID
    JOIN Triples T6     ON T5.subject=T6.subject
    JOIN Dictionary DP6 ON T6.predicate=DP6.ID
    JOIN Dictionary D3  ON T6.object=D3.ID
    JOIN Triples T7     ON T6.subject=T7.subject
    JOIN Dictionary DP7 ON T7.predicate=DP7.ID
    JOIN Triples T8     ON T7.object=T8.subject
    JOIN Dictionary DP8 ON T8.predicate=DP8.ID
    JOIN Dictionary D4  ON T8.object=D4.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:creator'
    AND DP3.val='foaf:name'
    AND DP4.val='swrc:journal'
    AND DP5.val='swrc:journal'
    AND DP6.val='rdf:type'
    AND DP7.val='dc:creator'
    AND DP8.val='foaf:name'
    AND D1.val='bench:Article'
    AND D3.val='bench:Article'
    AND D2.val<D4.val;


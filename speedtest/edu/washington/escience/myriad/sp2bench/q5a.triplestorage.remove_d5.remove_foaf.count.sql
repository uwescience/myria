select count(*) from (SELECT DISTINCT
    D7.val AS person,
    D6.val AS name
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID 
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID,
    Triples T4
    JOIN Dictionary DP4 ON T4.predicate=DP4.ID
    JOIN Dictionary D4  ON T4.object=D4.ID 
    JOIN Triples T5     ON T4.subject=T5.subject
    JOIN Dictionary DP5 ON T5.predicate=DP5.ID
    ,
    Triples T10
    Join Dictionary DP6 on T10.predicate=DP6.ID
    JOIN Dictionary D6 on T10.object=D6.ID
    JOIN Dictionary D7 on T10.subject=D7.ID
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:creator'
    AND DP4.val='rdf:type'
    AND DP5.val='dc:creator'
    AND DP6.val='foaf:name'
    AND D1.val='bench:Article'
    AND D4.val='bench:Inproceedings'
    AND T2.object=T5.object
    AND T2.object=T10.subject);


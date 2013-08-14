SELECT
    L1.yr       AS yr,
    L1.name     AS name,
    L1.document AS document 
FROM
    (
        SELECT
            T1.subject AS class,
            D2.val     AS document,
            D3.val     AS yr,
            D4.val     AS author,
            D5.val     AS name
        FROM
            Triples T1
            JOIN Dictionary DP1 ON T1.predicate=DP1.ID
            JOIN Dictionary D1  ON T1.object=D1.ID
            JOIN Triples T2     ON T1.subject=T2.object
            JOIN Dictionary DP2 ON T2.predicate=DP2.ID
            JOIN Dictionary D2  ON T2.subject=D2.ID
            JOIN Triples T3     ON T3.subject=T2.subject
            JOIN Dictionary DP3 ON T3.predicate=DP3.ID
            JOIN Dictionary D3  ON T3.object=D3.ID
            JOIN Triples T4     ON T4.subject=T3.subject
            JOIN Dictionary DP4 ON T4.predicate=DP4.ID
            JOIN Dictionary D4  ON T4.object=D4.ID
            JOIN Triples T5     ON T5.subject=T4.object
            JOIN Dictionary DP5 ON T5.predicate=DP5.ID
            JOIN Dictionary D5  ON T5.object=D5.ID
        WHERE
            DP1.val='rdfs:subClassOf'
            AND DP2.val='rdf:type'
            AND DP3.val='dcterms:issued'
            AND DP4.val='dc:creator'
            AND DP5.val='foaf:name'
            AND D1.val='foaf:Document'
    ) L1
    LEFT JOIN
    (
        SELECT
            T1.subject AS class,
            D2.val     AS document,
            D3.val     AS yr,
            D4.val     AS author
        FROM
            Triples T1
            JOIN Dictionary DP1 ON T1.predicate=DP1.ID
            JOIN Dictionary D1  ON T1.object=D1.ID
            JOIN Triples T2     ON T1.subject=T2.object
            JOIN Dictionary DP2 ON T2.predicate=DP2.ID
            JOIN Dictionary D2  ON T2.subject=D2.ID
            JOIN Triples T3     ON T3.subject=T2.subject
            JOIN Dictionary DP3 ON T3.predicate=DP3.ID
            JOIN Dictionary D3  ON T3.object=D3.ID
            JOIN Triples T4     ON T4.subject=T3.subject
            JOIN Dictionary DP4 ON T4.predicate=DP4.ID
            JOIN Dictionary D4  ON T4.object=D4.ID
        WHERE
            DP1.val='rdfs:subClassOf'
            AND DP2.val='rdf:type'
            AND DP3.val='dcterms:issued'
            AND DP4.val='dc:creator'
            AND D1.val='foaf:Document'
    ) L2
    ON L1.author=L2.author AND L2.yr<L1.yr
WHERE L2.author IS NULL;

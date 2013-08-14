SELECT
    D0.val AS inproc,
    D2.val AS author,
    D3.val AS booktitle,
    D4.val AS title,
    D5.val AS proc,
    D6.val AS ee,
    D7.val AS page,
    D8.val AS URL,
    D9.val AS yr,    
    AB.val AS abstract
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D0  ON T1.subject=D0.ID
    JOIN Dictionary D1  ON T1.object=D1.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Dictionary D2  ON T2.object=D2.ID
    JOIN Triples T3     ON T1.subject=T3.subject
    JOIN Dictionary DP3 ON T3.predicate=DP3.ID
    JOIN Dictionary D3  ON T3.object=D3.ID
    JOIN Triples T4     ON T1.subject=T4.subject
    JOIN Dictionary DP4 ON T4.predicate=DP4.ID
    JOIN Dictionary D4  ON T4.object=D4.ID
    JOIN Triples T5     ON T1.subject=T5.subject
    JOIN Dictionary DP5 ON T5.predicate=DP5.ID
    JOIN Dictionary D5  ON T5.object=D5.ID
    JOIN Triples T6     ON T1.subject=T6.subject
    JOIN Dictionary DP6 ON T6.predicate=DP6.ID
    JOIN Dictionary D6  ON T6.object=D6.ID
    JOIN Triples T7     ON T1.subject=T7.subject
    JOIN Dictionary DP7 ON T7.predicate=DP7.ID
    JOIN Dictionary D7  ON T7.object=D7.ID
    JOIN Triples T8     ON T1.subject=T8.subject
    JOIN Dictionary DP8 ON T8.predicate=DP8.ID
    JOIN Dictionary D8  ON T8.object=D8.ID
    JOIN Triples T9     ON T1.subject=T9.subject
    JOIN Dictionary DP9 ON T9.predicate=DP9.ID
    JOIN Dictionary D9  ON T9.object=D9.ID
    LEFT JOIN (
        SELECT *
        FROM
            Triples T10
JOIN Dictionary DP10 ON T10.predicate=DP10.ID
            JOIN Dictionary D10 ON T10.object=D10.ID
        WHERE
            DP10.val='bench:abstract'
    ) AB ON T1.subject=AB.subject
WHERE
    DP1.val='rdf:type'
    AND DP2.val='dc:creator'
    AND DP3.val='bench:booktitle'
    AND DP4.val='dc:title'
    AND DP5.val='dcterms:partOf'
    AND DP6.val='rdfs:seeAlso'
    AND DP7.val='swrc:pages'
    AND DP8.val='foaf:homepage'
    AND DP9.val='dcterms:issued'
    AND D1.val='bench:Inproceedings'
ORDER BY D9.val;


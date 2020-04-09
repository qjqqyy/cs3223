CS3223 Project
==============

Group members:
- Kerryn Eer (A0149960U)
- Manickamalar Jothinandan Pillay (A0168954L)
- Qi Ji (A0167793L)

For the project we implemented the following:

* Block Nested Loops Join
* Sort Merge Join (with external sort algorithm)
* `DISTINCT` operator (implemented with external sort)
* Aggregate functions (`MIN`, `MAX`, `COUNT`, `SUM`, `AVG`)
* Replaced the optimizer with another randomized algorithm
* Addressed some miscellaneous limitations

For more implementation details see corresponding section in report, see /report.pdf.

Usage
-----

We have left the user-facing components unchanged and did not modify the parser.
Original instructions from http://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm still apply,

    java RandomDB <tablename> <# records to generate>
    java ConvertTxtToTbl <table name>
    java ConvertTxtToTbl QueryMain query.in query.out

Notes
-----

New code written is only tested on **JDK version 9** and above.

### Special Case - Combination of Aggregated & Non-Aggregated Columns ###

As we did not implement `GROUP BY` and `ORDER BY`,
in the event where a query is called where there are both aggregated and non-aggregated,
it is dealt with the following rules

1. As long as there is even one aggregated column to be projected, as there no `GROUP BY` or `ORDER BY` clauses, only one tuple will be returned as the answer (assuming that the final aggregated value is not `NULL`).
2. The values of non-aggregated column projects will be random and undefined and the one tuple could contain any value from the column.

For example, for the following query:

    SELECT MAX(CUSTOMER.cid), COUNT(CUSTOMER.cid), CUSTOMER.cid
    FROM CUSTOMER

Only one tuple will be returned as there is at least one aggregated column for projection.
The value for `MAX(CUSTOMER.cid)` and `COUNT(CUSTOMER.cid)` will be calculated and output accordingly but the value for `CUSTOMER.cid` is only guaranteed to be from that particular column.

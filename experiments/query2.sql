SELECT Employees.eid,Employees.ename
FROM Employees,Certified,Schedule
WHERE Employees.eid=Certified.eid,Certified.aid=Schedule.aid,Schedule.flno="35777"

package sql42;

public class HrMin {
    public static class MyUdf {
        public Integer eval(Integer a) {
            return a + 1;
        }
    }

    public static class Nested {
        public final String fieldA;
        public final int fieldB;

        public Nested(String fieldA, int fieldB) {
            this.fieldA = fieldA;
            this.fieldB = fieldB ;
        }
    }

    public static class Employee {
        public final int empid;
        public final int secondid;
        public final String name;
        public final int depid;
        public final boolean good;
        public final int rating;

        public Employee(int empid, int secondid, String name, int depid, Boolean good, int rating) {
            this.empid = empid;
            this.secondid = secondid;
            this.name = name;
            this.depid = depid;
            this.good = good;
            this.rating = rating;
        }
    }

    public static class Department {
        public final int depid;
        public final int secondid;
        public final String name;
        public final Boolean useful;

        public Department(int depid, int secondid, String name, Boolean useful) {
            this.depid = depid;
            this.secondid = secondid;
            this.name = name;
            this.useful = useful;
        }
    }

    public static class NoobDep {
        public final int noobid;
        public final String name;
        public final Boolean boolval;
        public final Nested nested;
        public final Nested nested2;

        public NoobDep(int noobid, String name, Boolean boolval, Nested nested, Nested nested2) {
            this.noobid = noobid;
            this.name = name;
            this.boolval = boolval;
            this.nested = nested;
            this.nested2 = nested2;
        }
    }

    public static class Hr {
        public static final Employee[] emps = {
                new Employee(1, 1,  "Bill", 1, false, 3),
                new Employee(2, 1,  "Eric", 1, false, 2),
                new Employee(3, 1,  "Sebastian", 2, true, 4),
                new Employee(4, 1,  "GG", 3, true, 5),
                new Employee(5, 1,  "GGWP", 3, true, 4),
        };

        public static final Department[] deps = {
                new Department(1, 1,  "dep1", true),
                new Department(2, 1,  "dep2", true),
                new Department(3, 1,  "dep3", false),
        };

        public static final NoobDep[] noobs = {
                new NoobDep(1, "n7", true, new Nested("abc", 1), new Nested("abc", 1)),
                new NoobDep(2, "n2", true, new Nested("bec", 1), new Nested("abc", 1)),
                new NoobDep(3, "n3", false, new Nested("ef0", 2), new Nested("abc", 1)),
        };
    }
}


/*

SqlNode sqlNode = planner.parse("select depts.name, count(emps.empid) from emps inner join depts on emps.deptno = depts.deptno group by depts.deptno, depts.name order by depts.name");

select deps.depid, count(emps.empid) as count, max(emps.rating) from emps inner join deps on emps.depid = deps.depid group by deps.depid having count >= 2

dep_id, total_emps, max_rating
having total_emps >= 2

Join

Noob dep, name

dep_id, total_emps, max_rating, noob_name  where noob_id < 3


 */

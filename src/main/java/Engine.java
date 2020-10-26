import entities.Address;
import entities.Employee;
import entities.Project;
import entities.Town;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class Engine implements Runnable {
    private final EntityManager entityManager;
    private final Scanner scanner = new Scanner(System.in);

    public Engine(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public void run() {
        // 2. Remove Objects
        // this.removeObjectsExercise();

        // 3. Contains Employee
        // this.containsEmployeeExercise();

        // 4. Employees with Salary Over 50 000
        // this.employeesWithSalaryOverExercise();

        // 5. Employees from Department
        // this.extractEmployeesFromDeptExercise();

        // 6. Adding a New Address and Updating Employee
        // this.addAddressAndUpdateEmployeeExercise();

        // 7. Addresses with Employee Count
        // this.addressesWithEmployeeCountExercise();

        // 8. Get Employee with Project
        // this.getEmployeeWithProject();

        // 9. Find Latest 10 Projects
        // this.findLatest10ProjectsExercise();

        // 10. Increase Salaries
        // this.increaseSalariesExercise();

        // 11. Remove Towns
        // this.removeTownsExercise();

        // 12. Find Employees by First Name
        // this.findEmployeesByFirstNameExercise();

        // 13. Employees Maximum Salaries
        // employeesMaximumSalariesExercise();
    }

    // METHODS  ///////////////////////////////////////
    // 2. Remove Objects
    private void removeObjectsExercise() {
        this.entityManager.getTransaction().begin();
        this.entityManager.createQuery(
                "UPDATE Town t SET t.name = lower(t.name) " +
                        "WHERE length(t.name) <= 5")
                .executeUpdate();
        this.entityManager.getTransaction().commit();

    }

    // 3. Contains Employee
    private void containsEmployeeExercise() {
        System.out.println("Enter employee name: ");
        String name = scanner.nextLine();

        try {
            Object e = this.entityManager.createQuery(
                    "SELECT e FROM Employee e " +
                            "WHERE CONCAT(e.firstName, ' ', e.lastName) = :name")
                    .setParameter("name", name)
                    .getSingleResult();
        } catch (NoResultException nre) {
            System.out.println("No");
            return;
        }

        System.out.println("Yes");
    }

    // 4. Employees with Salary Over 50 000
    private void employeesWithSalaryOverExercise() {
        this.entityManager.createQuery(
                "SELECT e.firstName FROM Employee e " +
                        "WHERE e.salary > 50000", String.class)
                .getResultStream()
                .forEach(System.out::println);
    }

    // 5. Employees from Department
    private void extractEmployeesFromDeptExercise() {
        String deptName = "Research and Development";
        this.entityManager.createQuery(
                "SELECT e FROM Employee e " +
                        "JOIN e.department AS d " +
                        "WHERE d.name LIKE :deptName " +
                        "ORDER BY e.salary, e.id", Employee.class)
                .setParameter("deptName", deptName)
                .getResultStream()
                .forEach(e -> System.out.printf("%s %s from %s - $%.2f%n",
                        e.getFirstName(),
                        e.getLastName(),
                        e.getDepartment().getName(),
                        e.getSalary()));
    }

    // 6. Adding a New Address and Updating Employee
    private void addAddressAndUpdateEmployeeExercise() {
        System.out.println("Enter Employee's last name: ");
        String lastName = scanner.nextLine();

        Address address = addNewAddress("Vitoshka 15");

        try {
            this.entityManager.getTransaction().begin();
            Employee employee = this.entityManager.createQuery(
                    "SELECT e FROM Employee e " +
                            "WHERE e.lastName = :name", Employee.class)
                    .setParameter("name", lastName)
                    .getSingleResult();

            this.entityManager.detach(employee);
            employee.setAddress(address);
            this.entityManager.merge(employee);

            this.entityManager.flush();
            this.entityManager.getTransaction().commit();
        } catch (NoResultException nre) {
            System.out.printf("There are no records of Employee with last name %s%n", lastName);
        }
    }

    private Address addNewAddress(String text) {
        Address address = new Address();
        address.setText(text);

        this.entityManager.getTransaction().begin();
        this.entityManager.persist(address);
        this.entityManager.getTransaction().commit();

        return address;
    }

    // 7. Addresses with Employee Count
    private void addressesWithEmployeeCountExercise() {
        this.entityManager.createQuery(
                "SELECT a FROM Address a " +
                        "JOIN a.town " +
                        "JOIN a.employees AS e " +
                        "GROUP BY a " +
                        "ORDER BY COUNT(e) DESC", Address.class)
                .getResultStream()
                .limit(10)
                .forEach(a -> System.out.printf("%s, %s - %d employees%n",
                        a.getText(),
                        a.getTown().getName(),
                        a.getEmployees().size()));
    }

    // 8. Get Employee with Project
    private void getEmployeeWithProject() {
        System.out.println("Enter Employee's ID: ");
        Integer id = Integer.parseInt(scanner.nextLine());

        Employee e = this.entityManager.find(Employee.class, id);
        System.out.printf("%s %s - %s%n",
                e.getFirstName(),
                e.getLastName(),
                e.getJobTitle());
        e.getProjects().stream()
                .sorted(Comparator.comparing(Project::getName))
                .forEach(project -> System.out.printf("\t%s%n", project.getName()));
    }

    // 9. Find Latest 10 Projects
    private void findLatest10ProjectsExercise() {
        this.entityManager.createQuery(
                "SELECT p FROM Project AS p " +
                        "ORDER BY p.startDate DESC ", Project.class)
                .setMaxResults(10)
                .getResultStream()
                // .limit(10) // alternative to setMaxResults(10)
                .sorted(Comparator.comparing(Project::getName))
                .forEach(p -> System.out.printf("Project name: %s%n" +
                        " \tProject Description: %s%n" +
                        " \tProject Start Date:%s%n" +
                        " \tProject End Date: %s%n",
                        p.getName(),
                        p.getDescription(),
                        p.getStartDate(),
                        p.getEndDate()));
    }

    // 10. Increase Salaries
    private void increaseSalariesExercise() {

        this.entityManager.getTransaction().begin();
        List<Employee> employees = this.entityManager.createQuery(
                "SELECT e FROM Employee e " +
                        "JOIN e.department d " +
                        "WHERE d.name IN (" +
                        "'Engineering', " +
                        "'Tool Design', " +
                        "'Marketing', " +
                        "'Information Services')", Employee.class)
                .getResultList();

        for (Employee employee : employees) {
            this.entityManager.detach(employee);
            employee.setSalary(employee.getSalary().multiply(BigDecimal.valueOf(1.12)));
            System.out.printf("%s %s ($%.2f)%n",
                    employee.getFirstName(),
                    employee.getLastName(),
                    employee.getSalary());
            this.entityManager.merge(employee);
        }

        this.entityManager.getTransaction().commit();
    }

    // 11. Remove Towns
    private void removeTownsExercise() {
        System.out.println("Enter town name to be deleted: ");
        String townName = scanner.nextLine();

        Town town;

        try {
            town = this.getTownByName(townName);
        } catch (NoResultException nre) {
            System.out.println("No town found!");
            return;
        }

        List<Employee> employees = this.getEmployeesByTownName(town.getName());
        List<Address> addresses = this.getAddressesByTownName(town.getName());

        this.entityManager.getTransaction().begin();

        this.setTownToNull(addresses);
        this.setAddressesToNull(employees);
        this.deleteNulledAddresses(addresses);
        this.entityManager.remove(town);

        this.entityManager.getTransaction().commit();

        System.out.printf("%d address in %s deleted", addresses.size(), townName);

    }

    private void deleteNulledAddresses(List<Address> addresses) {
        for (Address address : addresses) {
            this.entityManager.remove(this.entityManager.contains(address)
                    ? address
                    : this.entityManager.merge(address));
        }
    }

    private void setTownToNull(List<Address> addresses) {
        for (Address address : addresses) {
            this.entityManager.detach(address);
            address.setTown(null);
            this.entityManager.merge(address);
        }
    }

    private void setAddressesToNull(List<Employee> employees) {
        for (Employee employee : employees) {
            this.entityManager.detach(employee);
            employee.setAddress(null);
            this.entityManager.merge(employee);
        }
    }

    private List<Address> getAddressesByTownName(String townName) {
        return this.entityManager.createQuery(
                "SELECT a FROM Address a " +
                        "JOIN a.town t " +
                        "WHERE t.name LIKE :name ", Address.class)
                .setParameter("name", townName)
                .getResultList();
    }

    private List<Employee> getEmployeesByTownName(String name) {
        return this.entityManager.createQuery(
                "SELECT e FROM Employee e " +
                        "JOIN e.address a " +
                        "JOIN a.town t " +
                        "WHERE t.name LIKE :name ", Employee.class)
                .setParameter("name", name)
                .getResultList();
    }

    private Town getTownByName(String townName) {
       return this.entityManager.createQuery(
                "SELECT t FROM Town t " +
                        "WHERE t.name LIKE :townName", Town.class)
                .setParameter("townName", townName)
                .getSingleResult();
    }

    // 12. Find Employees by First Name
    private void findEmployeesByFirstNameExercise() {
        System.out.println("Enter a pattern: ");
        String pattern = scanner.nextLine() + "%";

        this.entityManager.createQuery(
                "SELECT e FROM Employee e " +
                        "WHERE e.firstName LIKE :pattern", Employee.class)
                .setParameter("pattern", pattern)
                .getResultStream()
                .forEach(e -> System.out.printf("%s %s - %s - ($%.2f)%n",
                        e.getFirstName(),
                        e.getLastName(),
                        e.getJobTitle(),
                        e.getSalary()));
    }

    // 13. Employees Maximum Salaries
    private void employeesMaximumSalariesExercise() {
        this.entityManager.createQuery(
                "SELECT emp FROM Employee emp " +
                        "WHERE emp.salary IN (SELECT MAX(e.salary) " +
                        "               FROM Employee e " +
                        "               JOIN e.department d " +
                        "               GROUP BY d.name " +
                        "               HAVING MAX(e.salary) NOT BETWEEN 30000 AND 70000) " +
                        "GROUP BY emp.department", Employee.class)
                .getResultStream()
                .forEach(e -> System.out.printf("%s %.2f%n",
                        e.getDepartment().getName(),
                        e.getSalary()));
    }
}

# drill-jmh
JMH based framework to benchmark some of the Drill functionality

## ADDING NEW BENCHMARKS
To add new Drill JMH based benchmark logic, you can either add new benchmark methods to existing projects or create a new one.
Having multiple maven projects help keep the code well structured (similar functionality grouped together) and minimize the
build time.

### Add a new Maven project
* Edit the root pom.xml and add a new target module
* The name of the project here is not key as you can create a logical name within the child project's pom file
* Copy an existing child pom.xml to a new sub-directory and make the necessary changes
* The parent Drill JMH pom contains most of the dependency and build information so child pom changes should be minimal (e.g., artifact identifier, name,
  your benchmark FQN)
* Add state classes and utility logic under the "jmh-common" project to promote code reuse
* Add a new JMH benchmark class within you new project

## Running your benchmark
cd to your child project (let's use the drill-jmh-parquet project as an example)

a) cd parquet; java -jar ./target/drill-jmh-parquet-1.0.jar -h
to view the help

 -b         enable boundary checks
 
 -f <arg>   JMH test filter (default: .*)

 -h         display this help

 -L <arg>   set the maximum entry length (parquet tests; default: 1)

 -m <arg>   Number of Measure iterations (default: 10)

 -r         enable reference count checks

 -t         run single iteration for testing

 -V         enable variable length data (parquet tests)

 -w <arg>   Number of Warmup iterations (default: 5)

 This framework provides common options for all tests (checks enablement, test-mode, and JMH number of Warmup / iterations);
 though each project can register custom options (in this case, the -L and -V options).

b) To run the tests, type: java -jar ./target/drill-jmh-parquet-1.0.jar  (and any option you want to set)

## Editing and Running the Tests from Eclipse
* Run the following maven command from the top directory: mvn eclipse:clean && mvn eclipse:eclipse
* Launch Eclipse and import the project you want to edit / run
* The main goal of running from an IDE is to simplify debugging
* Select your benchmark class, right click, select Run As, Run Configurations
* Create a new Java Application runtime configuration; add -t as a program argument
* This will essentially run your test() method where you can debug newly or edited tests

**NOTE -** 
* If you need to run the JMH benchmark (non-test mode) from Eclipse, then you will need to manually add the \<artifact-id\>-\<version\>.jar JAR file to the Launcher class path
* In test mode, some of the options don't take effect as the initialization logic uses Java properties to pass such information
* To overcome this limitation, you can add such properties to the launcher configuration VM arguments (e.g., -Ddrill.jmh.parquet.entry_len=16 -Ddrill.jmh.parquet.varlen=true)

## TODO - Editing and Running from other editors

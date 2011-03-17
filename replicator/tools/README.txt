Module Structure
====
Configurator
- Modules (Loaded from the modules directory)
- Deployments (Loaded from the deployments directory)
- Prompt Handler
  - Prompts (Loaded from each module)
- Validation Handler
  - Validation Checks (Loaded from each module)
- Deployment Handler

Adding Prompts
====

The following example adds a prompt to collect the MySQL configuration file location.

Add these lines to register_prompts in configure_module_replicator.rb before the Connector/J path prompt.

MySQLConfigurePrompt.new(REPL_MYSQL_MYCNF, "MySQL configuration file", 
  PV_FILENAME, "/etc/my.cnf"),
  
Rebuild the tools package and run the script again  

Adding Validation Checks
====

The following example adds a check to verify that there are no bind address lines in the configuration file.

Add this class to validation_replicator_mysql.rb

class MySQLBindAddress < MySQLValidationCheck
  def set_vars
    @title = "MySQL bind address check"
  end
  
  def validate
    info("Checking for bind_address in #{@config.getProperty(REPL_MYSQL_MYCNF)}")
    bind_lines_count = cmd_result("cat #{@config.getProperty(REPL_MYSQL_MYCNF)} | grep bind | grep address | wc -l")
    
    if bind_lines_count.to_i() > 0
      error("There is a configuration value for bind_address")
      help("Remove any bind_address lines from #{@config.getProperty(REPL_MYSQL_MYCNF)}")
    end
  end
end

Add this line to register_validation_checks in configure_module_replicator.rb

MySQLBindAddress.new()

Rebuild the tools package and run the script again

Modifying an Existing Deployment Step
====

Each deployment step executes a set of methods defined inside of that module.  If there is a method that is modifying the file in question, extend that for your needs.  If you need to modify a new file or do something that is not already covered, you can create the new method and add it to the array returned by get_deployment_methods in that module.

Adding a Deployment Step
====

Create a new module file in the deployment_steps directory.  The new module must have a get_deployment_methods method and include the 'module_function :get_deployment_methods' line that make the method accessible.  After the module is defined you must include it in the return array from the get_deployment_object_modules method of the deployment you would like to change.
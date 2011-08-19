Module Structure
====
Configurator
- Packages
- Deployments
- Prompt Handler
  - Prompts (Loaded from the modules dir, selected by the package)
- Validation Handler
  - Validation Checks (Loaded from the modules dir, selected by the package)
- Deployment Handler

Adding Prompts
====

To add a prompt you must add a constant to parameter_names.rb and define a new class that extends ConfigurePrompt and includes one of three modules

* ClusterHostPrompt
* ReplicationServicePrompt
* DatasourcePrompt

These are the basic modules that will affect how a prompt is displayed in interactive mode, help output and the config file.  There are additional modules that can be included to modify behavior.

* AdvancedPromptModule - Only enabled when advanced mode is specified
* ConstantValueModule - Only available for template files.  Use this for values that are purely based on other prompts.
* You will see other modules included in prompts to modify the behavior

For any prompt you must at least define the the initialize method.

def initialize
  super(PARAMETER_NAME, "Description", PropertyValidator object, "Default Value")
end

The two most common actions after that will be to modify when the the prompt is enabled and dynamically identify the default value.  If you need to limit when the prompt is enabled you should extend the 'enabled?' and 'enabled_for_config?' methods.

The 'get_default_value' method will allow you to return the default value for the prompt.  This is useful when the default value is dependent on other config values, like directory names.

Adding Validation Checks
====

Including a Config Setting in a Template
====

Once you have defined a prompt, you can start to use the PARAMETER_NAME in your template files by specifying '@{PARAMETER_NAME}'.  You can see what template keys are available by running with the '--template-file-help'.

If you need a new template key that is based on existing prompts.  Add a new prompt that includes one of the core modules but also includes ConstantValueModule.  Define the 'get_default_value' method on that prompt to build the value which will be placed in the template.
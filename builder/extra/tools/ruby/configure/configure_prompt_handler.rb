# This class will prompt the user for each of the ConfigurePrompts that are
# registered and validate that they have valid values
class ConfigurePromptHandler
  def initialize(config)
    @errors = []
    @prompts = []
    @config = config
    initialize_prompts()
  end
  
  # Tell each ConfigureModule to register their prompts
  def initialize_prompts
    @prompts = []
    Configurator.instance.modules.each{
      |configure_module| 
      configure_module.register_prompts(self)
    }
    @prompts = @prompts.sort{|a,b| a.get_weight <=> b.get_weight}
  end
  
  def register_prompts(prompt_objs)
    prompt_objs.each{|prompt_obj| register_prompt(prompt_obj)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_prompt(prompt_obj)    
    unless prompt_obj.is_a?(ConfigurePromptInterface)
      raise "Attempt to register invalid prompt #{prompt_obj.class} failed " +
        "because it does not extend ConfigurePromptInterface"
    end
    
    prompt_obj.set_config(@config)
    unless prompt_obj.is_initialized?()
      raise "#{class_name} cannot be used because it has not been properly initialized"
    end
    
    @prompts.push(prompt_obj)
  end
  
  # Loop over each ConfigurePrompt object and collect the response
  def run()
    i = 0
    previous_prompts = []
    
    display_help()
    
    # Go through each prompt in the system and collect a value for it
    while i < @prompts.length
      begin
        @prompts[i].run()
        if @prompts[i].allow_previous?()
          previous_prompts.push(i)
        end
        i += 1
      rescue ConfigureSaveConfigAndExit => csce
        # Pass the request to save and exit up the chain untouched
        raise csce
      rescue ConfigureAcceptAllDefaults
        force_default_prompt = false
        # Store the default value for this and all remaining prompts
        Configurator.instance.write "Accepting the default value for all remaining prompts"
        while i < @prompts.length do
          unless @prompts[i].enabled?
            # Clear the config value because the prompt is disabled
            @prompts[i].save_disabled_value()
            i += 1
          else
            # Save the default value into the config
            @prompts[i].save_current_value()
            begin
              # Trigger the prompt to be run because the user requested it
              if force_default_prompt
                raise PropertyValidatorException
              end
              
              # Ensure that the value is valid 
              @prompts[i].is_valid?
              i += 1
            rescue PropertyValidatorException => e
              begin
                # Prompt the user because the default value is invalid
                @prompts[i].run
                if @prompts[i].allow_previous?
                  previous_prompts.push(i)
                end
                i += 1
                force_default_prompt = false
              rescue ConfigureSaveConfigAndExit => csce
                # Pass the request to save and exit up the chain untouched
                raise csce
              rescue ConfigureAcceptAllDefaults
                # We are already trying to do this
                puts "The current prompt does not have a valid default, please provide a value"
                redo
              rescue ConfigurePreviousPrompt
                previous_prompt = previous_prompts.pop()
                if previous_prompt == nil
                  puts "Unable to move to the previous prompt"
                else
                  i = previous_prompt
                  force_default_prompt = true
                end
              end
            end
          end
        end
      rescue ConfigurePreviousPrompt
        previous_prompt = previous_prompts.pop()
        if previous_prompt == nil
          puts "Unable to move to the previous prompt"
        else
          i = previous_prompt
        end
      end
    end
  end
  
  def is_valid?
    prompt_keys = []
    @errors = []

    # Test each ConfigurePrompt to ensure the config value passes the validation rule
    @prompts.each{
      |prompt|
      begin
        prompt_keys = prompt_keys + prompt.get_keys()
        prompt.is_valid?()
      rescue ConfigurePromptErrorSet => s
        @errors = @errors + s.errors
      rescue PropertyValidatorException => e
        @errors.push(ConfigurePromptError.new(prompt, e.to_s(), @config.getProperty(prompt.get_name())))
      end
    }
    
    # Ensure that there are not any extra values in the config object
    find_extra_keys = lambda do |hash, current_path|
      hash.each{
        |key,value|
        
        if current_path
          path = "#{current_path}.#{key}"
        else
          path = key
        end
        
        if value.is_a?(Hash)
          find_extra_keys.call(value, path)
        else
          unless prompt_keys.include?(path)
            @errors.push(ConfigurePromptError.new(
              ConfigurePrompt.new(path, "Unknown configuration key"),
              "This is an unknown configuration key",
              @config.getProperty(path.split("."))
            ))
          end
        end
      }
    end
    find_extra_keys.call(@config.props, false)
    
    if @errors.length == 0
      true
    else
      false
    end
  end
  
  def print_errors
    @errors.each{
      |error|
      Configurator.instance.write_divider(Logger::ERROR)
      Configurator.instance.error error.prompt.get_prompt()
      Configurator.instance.error "> Message: #{error.message}"
      Configurator.instance.error "> Config Key: #{error.prompt.get_name()}"
      
      if error.current_value.to_s() != ""
        Configurator.instance.error "> Current Value: #{error.current_value}"
      end
    }
    Configurator.instance.write_divider(Logger::ERROR)
  end
  
  def display_help
    filename = File.dirname(__FILE__) + "/interface_text/configure_prompt_handler_run"
    Configurator.instance.write_from_file(filename)
  end
end

class ConfigurePromptError
  attr_reader :prompt, :message, :current_value
  
  def initialize(prompt, message, current_value = nil)
    @prompt = prompt
    @message = message
    @current_value = current_value
  end
end

class ConfigurePromptErrorSet < StandardError
  attr_reader :errors
  
  def initialize(errors = [])
    @errors = errors
  end
end
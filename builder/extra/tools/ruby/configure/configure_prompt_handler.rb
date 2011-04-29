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
    Configurator.instance.package.get_prompts().each{
      |prompt_obj| 
      register_prompt(prompt_obj, @prompts)
    }
    @prompts = @prompts.sort{|a,b| a.get_weight <=> b.get_weight}
    
    @non_interactive_prompts = []
    Configurator.instance.package.get_non_interactive_prompts().each{
      |prompt_obj| 
      register_prompt(prompt_obj, @non_interactive_prompts)
    }
    @non_interactive_prompts = @non_interactive_prompts.sort{|a,b| a.get_weight <=> b.get_weight}
  end
  
  def register_prompts(prompt_objs)
    prompt_objs.each{|prompt_obj| register_prompt(prompt_obj, @prompts)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_prompt(prompt_obj, container)    
    unless prompt_obj.is_a?(ConfigurePromptInterface)
      raise "Attempt to register invalid prompt #{prompt_obj.class} failed " +
        "because it does not extend ConfigurePromptInterface"
    end
    
    prompt_obj.set_config(@config)
    unless prompt_obj.is_initialized?()
      raise "#{prompt_obj.class().name()} cannot be used because it has not been properly initialized"
    end
    
    container.push(prompt_obj)
  end
  
  # Loop over each ConfigurePrompt object and collect the response
  def run()
    i = 0
    previous_prompts = []
    
    display_help()
    
    # Go through each prompt in the system and collect a value for it
    while i < @prompts.length
      begin
        Configurator.instance.debug("Start prompt #{@prompts[i].class().name()}:#{@prompts[i].get_name()}")
        @prompts[i].run()
        Configurator.instance.debug("Finish prompt #{@prompts[i].class().name()}:#{@prompts[i].get_name()}")
        if @prompts[i].allow_previous?()
          previous_prompts.push(i)
        end
        i += 1
      rescue ConfigureSaveConfigAndExit => csce
        # Pass the request to save and exit up the chain untouched
        raise csce
      rescue ConfigureAcceptAllDefaults
        force_default_prompt = false
        
        fix_default_value = lambda do ||
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
            rescue ConfigurePromptError => e
              fix_default_value.call()
            rescue ConfigurePromptErrorSet => s
              fix_default_value.call()
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
    
    unless is_valid?()
      return false
    end
    
    true
  end
  
  def is_valid?
    validate_prompts(@non_interactive_prompts + @prompts)
  end
    
  def validate_prompts(prompts)
    prompt_keys = []
    @errors = []

    # Test each ConfigurePrompt to ensure the config value passes the validation rule
    prompts.each{
      |prompt|
      begin
        if prompt.enabled?()
          prompt.save_current_value()
        else
          prompt.save_disabled_value()
        end
        prompt_keys = prompt_keys + prompt.get_keys()
        prompt.is_valid?()
      rescue ConfigurePromptError => e
        @errors << e
      rescue ConfigurePromptErrorSet => s
        @errors = @errors + s.errors
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

class ConfigurePromptError < StandardError
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
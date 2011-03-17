package com.continuent.tungsten.commons.utils;

public class Password
{
    private String password = null;
    
    public Password(String password)
    {
        this.password = password;
    }
    
    public String toString()
    {
        return "**********";
    }

    /**
     * Returns the password value.
     * 
     * @return Returns the password.
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password value.
     * 
     * @param password The password to set.
     */
    public void setPassword(String password)
    {
        this.password = password;
    }
}

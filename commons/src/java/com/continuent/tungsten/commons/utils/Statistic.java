package com.continuent.tungsten.commons.utils;



public interface Statistic<T> 
{ 
   public void setValue(Number value);
    
   public T increment();
   
   public T add(Number value);
   
   public T decrement();
   
   public T subtract(Number value);
   
   public T getValue();
   
   public T getAverage();
   
   public String getLabel();
   
   public void clear();
    
}

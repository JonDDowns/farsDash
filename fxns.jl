# Function to convert a datetime (missing if error)
dtWithMissing = function(yr, mo, dy, hr, min)        
    try 
        return(DateTime(yr, mo, dy, hr, min))        
    catch         
        return(missing)
    end    
end
# Load packages
using CSV, DataFrames, ZipFile, FreqTables, PCRE2, Dates, StatsBase, Plots
include("fxns.jl")

# Identify all zip files in directory that aren't aux
# dataDir = "/run/media/jondowns/TOSHIBA EXT/FARS/"
dataDir = "/run/media/jondowns/TOSHIBA EXT/FARS/"
farsZips = filter(
    x -> !occursin("Auxiliary", x) 
    & occursin("zip", x),
    readdir(dataDir)
)

# Load list of wanted vars for final DF
keepVars = CSV.read("keepVars.csv", DataFrame)

# Read in DFs, do basic transforms and subsetting, create combined object
dfs = []
colsToInt = [
    :YEAR, :MONTH, :DAY, :HOUR, :MINUTE, :AGE, 
    :PER_TYP, :REST_USE
]

readFarsZip = function(dataDir, farsZip, keepVars, colsToInt)    
    # Identify person file (main)    
    zarc = ZipFile.Reader(dataDir * farsZip)
    persFn = filter(
        x -> occursin("PERSON.CSV", uppercase(x.name)),
        zarc.files)
    
    # Read in dataframe, format, and add to main DF list
    df = CSV.read(persFn, DataFrame, types=String)
    df[!, :file] .= farsZip
    df[!, :YEAR] .= match(r"[0-9]{4}", farsZip).match    

    # Rename columns as needed
    aliases = dropmissing(keepVars, :alias)
    toRename = names(df)[map(x -> x in aliases[!, :alias], names(df))]    
    for nm in toRename        
        newName = aliases[map(x -> x == nm, aliases[!, :alias]), :varName][1]
        df = rename(df, nm => newName)
    end
    
     # Add missing columns, subset dataframe
     addCols = setdiff(keepVars[!, :varName], names(df))
     for c in addCols
         df[!, c] .= missing
     end
     df = df[!, keepVars[!, :varName]]

     # Convert date parts to numeric
     df = transform!(
         df,
         colsToInt .=> (x -> parse.(Int64, x)) .=> colsToInt
     )
end

# Run function for each file
for farsZip in farsZips
    try        
        df = readFarsZip(dataDir, farsZip, keepVars, colsToInt)
        push!(dfs, df)
    catch
        try 
            println(farsZip * " attempt #2")
            sleep(2)
            df = readFarsZip(dataDir, farsZip, keepVars, colsToInt)
            push!(dfs, df)
        catch
            println(farsZip * " failed")
        end
    end
end
allDat = reduce(vcat, dfs)

# Calcultate the datetime variable -- currently missing for all
allDat = transform(
    allDat,
    :YEAR, :MONTH, :DAY, :HOUR, :MINUTE, 
    [:YEAR, :MONTH, :DAY, :HOUR, :MINUTE] => (
        (YEAR, MONTH, DAY, HOUR, MINUTE) -> 
        dtWithMissing.(YEAR, MONTH, DAY, HOUR, MINUTE)
    ) => :"eventTime"
)

# And get restraint use 
function carRest(AGE, REST_USE, PER_TYP, YEAR)
    if PER_TYP ∉ [1, 2]
        return (missing)
    if YEAR < 1991
        return (missing)
    end
    elseif YEAR < 1993 
        if AGE < 10 & REST_USE != 4 
            return (0)
        end
    else
        return (999)
    end
end

select(
    allDat,
    [:AGE, :REST_USE, :PER_TYP, :YEAR],
    [:AGE, :REST_USE, :PER_TYP, :YEAR] => (
        (AGE, REST_USE, PER_TYP, YEAR) -> carRest.(AGE, REST_USE, PER_TYP, YEAR)
    )
)
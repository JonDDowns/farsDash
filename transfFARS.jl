# Load packages
using CSV, DataFrames, ZipFile, FreqTables, PCRE2_jll

# Identify all zip files in directory that aren't aux
# dataDir = "/run/media/jondowns/TOSHIBA EXT/FARS/"
dataDir = "D:/FARS/"
farsZips = filter(
    x -> !occursin("Auxiliary", x) 
    & occursin("zip", x),
    readdir(dataDir)
)

# Load list of wanted vars for final DF
keepVars = CSV.read("keepVars.csv", DataFrame)

# Read in the person dataframe of each file
dfs = []
for farsZip in farsZips
    # Identify person file (main)    
    zarc = ZipFile.Reader(dataDir * farsZip)
    persFn = filter(
        x -> occursin("PERSON.CSV", uppercase(x.name)),
        zarc.files)
    try        
        # Read in dataframe, format, and add to main DF list
        df = CSV.read(persFn, DataFrame, types=String)
        df[!, "file"] .= farsZip
        df[!, "YEAR"] .= match(r"[0-9]{4}", farsZip).match
        addCols = setdiff(keepVars[!,"varName"], names(df))
        for c in addCols
            df[!, c] .= missing
        end
        df = df[!, keepVars[!, "varName"]]
        push!(dfs, df)
    catch
        println(farsZip)
    end
end

# Collpase into a single dataframe
using Dates
allDat = reduce(vcat, dfs)
names(allDat)
hi .= DateTime(parse(Int64, allDat[!, "YEAR"]), parse(Int64, allDat[!, "MONTH"]), 1)
DateTime(12, 1, 3)
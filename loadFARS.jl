# Load packages
using CSV, DataFrames, ZipFile, FreqTables

# Identify all zip files in directory that aren't aux
dataDir = "/run/media/jondowns/TOSHIBA EXT/FARS/"
farsZips = filter(
    x -> !occursin("Auxiliary", x) 
    & occursin("zip", x),
    readdir(dataDir)
)

# Read in the person dataframe of each file
dfs = []
for farsZip in farsZips    
    zarc = ZipFile.Reader(dataDir * farsZip)
    persFn = filter(
        x -> occursin("PERSON.CSV", uppercase(x.name)),
        zarc.files)
    try        
        df = CSV.read(persFn, DataFrame)
        df[!, "file"] .= farsZip
        push!(dfs, df)
    catch
        println(farsZip)
    end
end

varNames = map(
    x -> DataFrame(
        file = x[1,"file"],
        varName = names(x)), dfs
)
varNames = vcat(varNames...)

gdf = groupby(varNames, [:varName])
varCounts = combine(
    gdf,
    nrow => :count,
    :file => minimum => :firstFile,
    :file => maximum => :lastFile)
CSV.write("varCounts.csv", varCounts)
# Load packages
using HTTP
using Gumbo
using AbstractTrees
using DelimitedFiles

# Directory for output
outDir = "/run/media/jondowns/TOSHIBA EXT/FARS/"

# Navigate to base FTP page, read in HTML data
baseUrl = "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"

# Pull/parse HTML
r = HTTP.request("GET", baseUrl)
doc = parsehtml(String(r.body))

# Function to extract all links from parsed HTML
pullLnks = function(doc)
    allRefs = []
    for elem in PreOrderDFS(doc.root)
        for child in AbstractTrees.children(elem)
            try
                if tag(child) == :a                
                    push!(allRefs, getattr(child, "href"))
                end
            catch
            end
        end
    end
    return allRefs
end

# Get all references from FARS FTP landing, restrict to FARS data links
allRefs = pullLnks(doc)
dlRefs = filter(x -> occursin(r"FARS/[0-9]{4}", x), allRefs)
dlRefs = unique!(dlRefs)

# Map url correctly, save to file to explore
dlRefsFull = map((x) -> "https://www.nhtsa.gov/" * x * "National/", dlRefs)

# Note all CSV files in the data
for lnk in dlRefsFull
    lnkReq = HTTP.request("GET", lnk)
    bod = parsehtml(String(lnkReq.body))
    farsData = filter(x -> occursin(r"https.*CSV", x), pullLnks(bod))
    farsData = filter(x -> !isfile(outDir * basename(x)), farsData)
    map(x -> download(x, outDir * basename(x)), farsData)
end



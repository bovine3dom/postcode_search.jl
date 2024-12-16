#!/bin/julia

using CSV, DataFrames, Statistics, Arrow, HivePartitioner

# read CSV with no column names
df = CSV.read("data/codepoint.csv", DataFrame, header=0) # cat *.csv > data/codepoint.csv # codepoint splits the csvs into tiny files
rename!(df, :Column1 => :postcode)
df.firstbit = getindex.(df[!, :postcode], Ref(1:3))

stats = combine(groupby(df, :firstbit), nrow)

quantile(stats.nrow, 0.99) # 8k so we're fine

writehivedir((path, table)->Arrow.write(path, table), "postcodes_partitioned", df[!, [:postcode, :firstbit]], [:firstbit]; filename="part0.arrow")

# Add directory listing file - maybe this should be added to HivePartitioner.jl?
foreach(x -> begin
        (root, dirs, files) = x
        # @show root, dirs 
        colsvals = filter(x-> length(x) == 2, split.(dirs, '='))
        col = getindex.(colsvals, 1) |> first # maybe throw one day if uniqueness is violated
        vals = getindex.(colsvals, 2)
        Arrow.write(joinpath(root, "directory_contents.arrow"), DataFrame(Symbol(col)=>vals))
end, Iterators.filter(x->length(x[2]) > 0, walkdir("postcodes_partitioned")))

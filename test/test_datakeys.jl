include(joinpath(@__DIR__,"..","src","DataKeys.jl"))

using .DataKeys

using Dates, UUIDs, Random, DataFrames


const w0 = Date(2018,2,5)

const t0 = DateTime(2018,10,31)


df = DataFrame(week=[fill(w0, 10); fill(w0+Week(1), 10); fill(w0+Week(2), 10)],
               A=rand(1:3, 30),
               t=(t0 .+ Minute.(rand(1:256, 30))),
               x=rand(30)
              )

k = PrimaryKey((week=w0,))

DataKeys.spawn(df::AbstractDataFrame, k::PrimaryKey) = SecondaryKey((A=nothing,))

function DataKeys.resolve(df::AbstractDataFrame, k::PrimaryKey)
    sk = DataKeys.spawn(df, k)
    function (sdf::AbstractDataFrame)
        τ = maximum(sdf.t)
        sdf = filter(r -> r.t == τ, sdf)
        sdf[filter(!∈(keys(sk)), names(sdf))]
    end
end


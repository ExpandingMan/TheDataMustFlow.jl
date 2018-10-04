module DataKeys
using UUIDs, Dates
using DataFrames # ultimately, should use Tables.jl


abstract type DataKey end


struct PrimaryKey{T<:NamedTuple} <: DataKey
    id::UUID
    t0::Int
    metadata::T

    PrimaryKey(id::UUID, t0::Integer, m::T) where {T<:NamedTuple} = new{T}(id, t0, m)
end

PrimaryKey(id::UUID, m::T) where {T <: NamedTuple} = PrimaryKey(id, round(Int,datetime2unix(now())), m)
PrimaryKey(m::T) where {T <: NamedTuple} = PrimaryKey(uuid1(), m)

Base.keys(k::PrimaryKey) = keys(k.metadata)

struct SecondaryKey{N,T<:NamedTuple} <: DataKey
    metadata::T

    SecondaryKey(m::T) where {N,T<:NamedTuple{N}} = new{N,T}(m)
end

Base.keys(k::SecondaryKey) = keys(k.metadata)
Base.values(k::SecondaryKey) = values(k.metadata)



keymatch(m::NamedTuple{N}, obj) where {N} = all(getproperty(obj, n) == getproperty(m, n) for n ∈ N)
keymatch(m::NamedTuple{N}, dict::AbstractDict) where {N} = all(dict[n] == getproperty(m, n) for n ∈ N)

keymatch(k::SecondaryKey, obj) = keymatch(k.metadata, obj)


for (fname, bname) ∈ [(:keyfilter, :filter), (:keyfilter!, :filter!)]
    @eval $fname(m::NamedTuple{N}, df::AbstractDataFrame) where {N} = $bname(r -> keymatch(m, r), df)

    # the default definition here is probably inadequate for the PrimaryKey
    @eval $fname(k::DataKey, df::AbstractDataFrame) = $fname(k.metadata, df)
end

function DataFrames.by(f::Function, df::AbstractDataFrame, k::PrimaryKey; sort::Bool=false)
    by(f, df, collect(keys(k)), sort=sort)
end
function DataFrames.by(df::AbstractDataFrame, k::PrimaryKey, f::Function; sort::Bool=false)
    by(df, collect(keys(k)), f, sort=sort)
end

function spawn end
function resolve end

function resolve(df::AbstractDataFrame, k::PrimaryKey, cols::AbstractVector{Symbol}; sort::Bool=false)
    by(resolve(df, k), df, cols, sort=sort)
end

function Base.get(df::AbstractDataFrame, k::PrimaryKey; sort::Bool=false)
    sk = spawn(df, k)
    df = keyfilter(k, df)
    resolve(df, k, collect(keys(sk)), sort=sort)
end



export DataKey, PrimaryKey, SecondaryKey

end

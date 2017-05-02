
colidx(sch::Data.Schema, n::Union{String,Symbol}) = sch[string(n)]
function colidx{T<:Union{String,Symbol}}(sch::Data.Schema, cols::AbstractVector{T})
    Int[colidx(sch,n) for n ∈ cols]
end

dictfunc(f::Function) = f
dictfunc{K,V}(dict::Dict{K,V})::V = (k::K -> dict[k])
dictfunc{K,V}(dict::Dict{K,V}, d::V)::V = (k::K -> get(dict, k, d))

Base.identity(vs...) = vs
# ident_vec(vs...) = Any[v for v ∈ vs]


#=========================================================================================
    <BatchIterator>
=========================================================================================#
function _N_batches(idx::AbstractVector{<:Integer}, ℓ::Integer)
    (l, m) = divrem(length(idx), ℓ)
    l + Int(m > 0)
end


# TODO consider implementing entire AbstractVector interface
# TODO handle truncations
struct BatchIterator{T<:Integer,K<:AbstractVector{T}}
    idx::K
    ℓ::Int  # batch length

    N::Int  # number of batches

    function (BatchIterator{T,K}(idx::AbstractVector{T}, ℓ::Integer)
              where {T<:Integer,K<:AbstractVector{T}})
        new(convert(K, idx), ℓ, _N_batches(idx, ℓ))
    end
end

function BatchIterator{T<:Integer,K<:AbstractVector{T}}(idx::K, ℓ::Integer)
    BatchIterator{T,K}(idx, ℓ)
end


Base.start(iter::BatchIterator) = 1

function Base.next{T,K<:AbstractUnitRange{T}}(iter::BatchIterator{T,K}, state::Int)
    a = iter.idx[1] + iter.ℓ*(state-1)
    b = min(iter.idx[end], iter.idx[1] + iter.ℓ*state - 1)
    (a:b, state+1)
end

function Base.next{T,K<:AbstractVector{T}}(iter::BatchIterator{T,K}, state::Int)
    a = iter.ℓ*(state-1) + 1
    b = min(length(iter.idx), iter.ℓ*state)
    (iter.idx[a:b], state+1)
end

Base.done(iter::BatchIterator, state::Integer) = state > iter.N
Base.length(iter::BatchIterator) = iter.N


"""
# `batchiter`
    batchiter([f::Function], idx::AbstractVector{<:Integer}, batch_size::Integer)

Returns an iterator over batches.  If a function is provided, this will apply the function
to the batches created from the indices `idx` with batch size `batch_size`.
"""
function batchiter(idx::AbstractVector{<:Integer}, batch_size::Integer=DEFAULT_BATCH_SIZE)
    BatchIterator(idx, batch_size)
end
# this is the generic batch iteration function
function batchiter(f::Function, idx::AbstractVector{<:Integer}, batch_size::Integer)
    (f(batch_idx) for batch_idx ∈ batchiter(idx, batch_size))
end
function batchiter(f::Function, idx::AbstractVector{<:Integer};
                   batch_size::Integer=DEFAULT_BATCH_SIZE)
    batchiter(f, idx, batch_size)
end
export batchiter
#=========================================================================================
    </BatchIterator>
=========================================================================================#



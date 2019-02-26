
#===========================================================================================
    <tags>
    Tools for declaring tags
===========================================================================================#
macro tag(name::Symbol)
    esc(:(struct $name <: Tag end))
end

macro tabletag(name::Symbol)
    esc(:(struct $name <: TableTag end))
end
#===========================================================================================
    </tags>
===========================================================================================#

#============================================================================================
    <basic interface>
============================================================================================#
metadata(ds::AbstractDataSet) = ds.metadata

Base.keys(ds::AbstractDataSet) = keys(ds.data)
Base.getindex(ds::AbstractDataSet, ::Type{T}) where {T<:Tag} = ds.data[T]
Base.setindex!(ds::AbstractDataSet, v, ::Type{T}) where {T<:Tag} = setindex!(ds, v, T)

Base.empty(ds::AbstractDataSet) = empty(ds.data)
Base.empty!(ds::AbstractDataSet) = empty!(ds.data)
Base.length(ds::AbstractDataSet) = length(ds.data)

"""
    alltags(ds::AbstractDataSet)

Returns a complete list of relevant tags for dataset `ds`.

This should be defined when implementing TheDataMustFlow interface.
"""
alltags(ds::AbstractDataSet) = Type[]

"""
    iscomplete(ds::AbstractDataSet, tags=alltags(ds))

Checks whether the dataset `ds` has all tags listed by `tags`.  By default, this will be
whatever is defined as the complete set of tags by the `alltags` function.
"""
function iscomplete(ds::AbstractDataSet, tags::AbstractVector=alltags(ds))
    all(t ∈ tags for t ∈ keys(ds))
end

"""
    initialize!(ds::AbstractDataSet)
    initialize!(dr::DataRemote, ds::AbstractDataSet)

Perform initialization actions on dataset `ds`.
"""
initialize!(dr::DataRemote, ds::AbstractDataSet) = ds
initialize!(ds::AbstractDataSet) = ds
#============================================================================================
    </basic interface>
============================================================================================#

#============================================================================================
    <loading>
============================================================================================#
"""
    load(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag}

Load the object tagged by `T` from the remote source `dr` using metadata from dataset
`ds`.
"""
load(dr::DataRemote, m::AbstractMetaData, ::Type{<:Tag}) = missing
load(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag} = load(dr, ds.metadata, T)

function load(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:TableTag}
    v = loadinit(dr, ds, T)
    ismissing(v) && (return missing)
    v = prep(dr, ds, T, v)
    v = aggregate(dr, ds, T, v)
    postprep(dr, ds, T, v)
end

prep(dr::DataRemote, ds::AbstractDataSet, ::Type{<:Tag}, v) = v
aggregate(dr::DataRemote, ds::AbstractDataSet, ::Type{<:Tag}, v) = v
postprep(dr::DataRemote, ds::AbstractDataSet, ::Type{<:Tag}, v) = v

"""
    load!(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag}

Load the object tagged by `T` from the remote source `dr` using metadata from dataset `ds`
and store it in the dataset.  Will be skipped silently if object is `missing`.

# Example
```julia
load!(dr, ds, T)
ds[T]  # this now retrieves the object tagged by `T`
```
"""
function load!(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag}
    v = load(dr, ds, T)
    ismissing(v) || (ds[T] = v)
    v
end

"""
    load!(dr::DataRemote, ds::AbstractDataSet, tags=alltags(ds))

Load data tagged by the tags `tags` from the source `dr` into the dataset `ds`.
By default this will be done for the set of tags returned by `alltags`.
"""
function load!(dr::DataRemote, ds::AbstractDataSet, tags::AbstractVector=alltags(ds))
    for t ∈ tags
        load!(dr, ds, t)
        @info("Loaded $t")
    end
    ds
end
#============================================================================================
    </loading>
============================================================================================#

#============================================================================================
    <saving>
============================================================================================#
"""
    save(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag}

Save data tagged by `T` in the dataset `ds` to `dr`.  By default this function will call
back to a call to `save(dr, T, ds[T])`.  That method should be defined by packages
implementing TheDataMustFlow interface.
"""
save(dr::DataRemote, ds::AbstractDataSet, ::Type{T}) where {T<:Tag} = save(dr, T, ds[T])

function save(dr::DataRemote, ds::AbstractDataSet, tags::AbstractVector=alltags(ds))
    for t ∈ tags
        save(dr, ds, t)
        @info("Saved $t")
    end
    ds
end
#============================================================================================
    </saving>
============================================================================================#


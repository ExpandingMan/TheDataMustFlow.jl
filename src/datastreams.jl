#=========================================================================================
    DataStreams Interface

        Here we re-implement the datastreams interface for wrapper types within
        TheDataMustFlow.
=========================================================================================#


Data.schema(m::AbstractMorphism) = Data.schema(m.s)
Data.schema{T<:Data.StreamType}(m::AbstractMorphism, ::Type{T}) = Data.schema(m.s, T)


#=========================================================================================
    <Pull>
=========================================================================================#
Data.isdone(m::AbstractMorphism{Pull}, row, col) = Data.isdone(m.s, row, col)
Base.size(m::AbstractMorphism{Pull}) = Base.size(m.s)
function Data.streamtype{T<:Data.StreamType}(m::AbstractMorphism{Pull}, ::Type{T})
    Data.streamtype(typeof(m.s), T)
end

function Data.streamfrom{T}(m::AbstractMorphism{Pull}, ::Type{Data.Field}, ::Type{T}, row, col)
    Data.streamfrom(m.s, Data.Field, T, row, col)
end
function Data.streamfrom{T}(m::AbstractMorphism{Pull}, ::Type{Data.Column}, ::Type{T}, col)
    Data.streamfrom(m.s, Data.Column, T, col)
end
#=========================================================================================
    </Pull>
=========================================================================================#


#=========================================================================================
    <Push>
=========================================================================================#
function Sink{T<:Data.StreamType}(snk, sch::Data.Schema, ::Type{T}, append::Bool,
                                  ref::Vector{UInt8})
    # TODO implement this!
end

Data.streamtypes(m::AbstractMorphism{Push}) = Data.streamtypes(typeof(m.s))

function Data.streamto!{T}(m::AbstractMorphism{Push},
                           ::Type{Data.Field}, val::T, row, col)
    Data.streamto!(m.s, Data.Field, val, row, col)
end
function Data.streamto!{T}(m::AbstractMorphism{Push},
                           ::Type{Data.Field}, val::T, row, col, sch::Data.Schema)
    Data.streamto!(m.s, Data.Field, val, row, col, sch)
end

function Data.streamto!{T}(m::AbstractMorphism{Push},
                           ::Type{Data.Column}, column::T, row, col)
    Data.streamto!(m.s, Data.Column, column, row, col)
end
function Data.streamto!{T}(m::AbstractMorphism{Push},
                           ::Type{Data.Column}, column::T, row, col,
                           sch::Data.Schema)
    Data.streamto!(m.s, Data.Column, column, row, col, sch)
end
#=========================================================================================
    <Push>
=========================================================================================#



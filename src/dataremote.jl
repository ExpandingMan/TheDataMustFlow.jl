
struct Directory <: DataRemote
    path::String
end

Base.isdir(dir::Directory) = isdir(dir.path)
Base.cd(dir::Directory) = cd(dir.path)
Base.cd(f::Function, dir::Directory) = cd(f, dir.path)
Base.readdir(dir::Directory) = readdir(dir.path)
Base.walkdir(dir::Directory; kwargs...) = walkdir(dir.path; kwargs...)
Base.mkdir(dir::Directory; kwargs...) = mkdir(dir.path; kwargs...)
Base.mkpath(dir::Directory; kwargs...) = mkpath(dir.path; kwargs...)
Base.chmod(dir::Directory, mode::Integer; kwargs...) = chmod(dir.path, mode; kwargs...)
Base.chown(dir::Directory, owner::Integer, group::Integer=-1) = chown(dir.path, owner, group)
Bsae.stat(dir::Directory) = stat(dir.path)
Base.rm(dir::Directory; kwargs...) = rm(dir.path; kwargs...)


"""
    directory!(path::AbstractString)

Check if an directory exists at `path`, if not create it, and return a `Directory` object
with path `path`.
"""
function directory!(path::AbstractString)
    isdir(path) && (return Directory(path))
    mkpath(path)
    Directory(path)
end

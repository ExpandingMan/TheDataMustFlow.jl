#!$JULIA_HOME/julia
using TheDataMustFlow
using Documenter

makedocs()

fname = joinpath("build", "README.md")
if isfile(fname)
    info("Updating README...")
    cp(fname, joinpath("..", "README.md"), remove_destination=true)
end

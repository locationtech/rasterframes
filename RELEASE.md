# RasterFrames Release Process

1. Make sure `release-notes.md` is updated.
2. Use `git flow release start x.y.z` to create release branch.
3. Manually edit `version.sbt` and `version.py` to set value to `x.y.z` and commit changes.
4. Do `docker login` if necessary.
5. `sbt` shell commands:  
    a. `clean`  
    b. `test it:test`  
    c. `makeSite`  
    d. `publishSigned` (LocationTech credentials required)  
    e. `sonatypeReleaseAll`. It can take a while, but should eventually show up [here](https://search.maven.org/search?q=g:org.locationtech.rasterframes).  
    f. `docs/ghpagesPushSite`  
    g. `rf-notebook/publish`  
6. `cd pyrasterframes/target/python/dist`
7. `python3 -m twine upload pyrasterframes-x.y.z-py3-none-any.whl`
8. Commit any changes that were necessary.
9. `git-flow finish release`. Make sure to push tags, develop and master
    branches.
10. On `develop`, update `version.sbt` and `version.py` to next development
    version (`x.y.(z+1)-SNAPSHOT` and `x.y.(z+1).dev0`). Commit and push.
11. In GitHub, create a new release with the created tag. Copy relevant
    section of release notes into the description.

module Infrastructure.GITActor

open Akkling
open Domain.Common

type Command =
| Commit of string * string * string * string * (string -> string -> unit) // folder * file * name * comment * logger
| AddAndCommit of string * string * string * string * (string -> string -> unit) // folder * file * name * comment * logger

let private gitActorBody (ctx : Actor<Command>) =

    let rec loop () = actor {

        let! command = ctx.Receive()

        match command with
        | Commit (folder, file, name, comment, logger) ->
            file
            |> GIT.commitFile folder comment
            |> Result.mapError (logger name)
            |> ignore
            return! loop ()

        | AddAndCommit (folder, file, name, comment, logger) ->
            result {
                let! _ = 
                    file
                    |> GIT.addFile folder
                    |> Result.mapError (logger name)
                let! _ =
                    file
                    |> GIT.commitFile folder comment
                    |> Result.mapError (logger name)
                return ()
            } |> ignore
            return! loop ()
    }
    loop ()

let spawn actorFactory =
    Akkling.Spawn.spawn actorFactory "GIT" <| props gitActorBody

let spawnForTests rootFolder actorFactory =
    let rec deleteFolder folder =
        let mutable options = System.IO.EnumerationOptions()
        options.AttributesToSkip <- enum<System.IO.FileAttributes> 0

        System.IO.Directory.EnumerateDirectories(folder, "*", options)
        |> Seq.iter deleteFolder

        System.IO.Directory.EnumerateFiles(folder, "*", options)
        |> Seq.iter (fun file ->
            System.IO.File.SetAttributes(file, System.IO.FileAttributes.Normal)
            System.IO.File.Delete file)

        System.IO.Directory.Delete folder

    let initRepo folder =
        System.IO.Directory.CreateDirectory(folder) |> ignore
        let gitFolder = System.IO.Path.Combine(folder, ".git")
        if (System.IO.Directory.Exists gitFolder) then deleteFolder gitFolder
        Infrastructure.GIT.initRepo folder |> Result.mapError failwith |> ignore
    
    initRepo rootFolder
    Akkling.Spawn.spawn actorFactory "GIT_tests" <| props gitActorBody

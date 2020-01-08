module Infrastructure.Common

let toLocalTimeString (dateTime:System.DateTime) =
    dateTime.ToLocalTime().ToString("f")

let countdownInHours (dateTime:System.DateTime) =
    let diff = dateTime - System.DateTime.UtcNow
    System.Math.Max(diff.TotalHours, 0.)

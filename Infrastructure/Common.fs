module Infrastructure.Common

let toLocalTimeString culture (dateTime:System.DateTime) =
    dateTime.ToLocalTime().ToString("f", culture)

let countdownInHours (dateTime:System.DateTime) =
    let diff = dateTime - System.DateTime.UtcNow
    System.Math.Max(diff.TotalHours, 0.)

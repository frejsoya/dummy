# Project Initialization

The project should be run in VS Code Dev Container. (Ctrl + Shift + P > Reopen Folder in Dev Container)

1. Create local database with (run inside the Postgres docker container) if does not exist:

```
su - postgres
createdb postgres
```

2. Initialize DB scheme:

```
flask db migrate
flask db upgrade
```

3. Run application from VS Code "Run and Debug" tab - "Python: Flask"


## Recovery of lost completed_workouts

![your-UML-diagram-name](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/frejsoya/dummy/master/state-dia.uml)

The above diagram shows the state transitions and actions taken, if any, for that state transition. It is only  valid for the events for one given specific user_id.
The states are:

- Init
- Start
- Phases

The transitions that change state are
- Start_workout
- Phase_progression_K
- Complete_workout

Note that events such as `readTip` are self-transition events and not shown in the diagram.
The End transition is a fake event_log transition, to signify no more events exit for that user_id.

Finally , there are only two transitions with a given action. Ie. when a new complete_workout is added.














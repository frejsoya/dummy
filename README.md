# Project Initialization

The project should be run in VS Code Dev Container. (Ctrl + Shift + P > Reopen Folder in Dev Container)

1. Create local database with (run inside the Postgres docker container) if does not exist:

```
su - postgres
createdb postgres
```

2. Initialize DB scheme:

```
flask db upgrade
```

3. Run application from VS Code "Run and Debug" tab - "Python: Flask"


## Recovery of lost completed_workouts

![your-UML-diagram-name](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/frejsoya/dummy/master/state-dia.uml)


The above diagram shows the state transitions and actions taken for that state transition. The diagram is valid for the events of one specific user_id.
The "End" transition is when no more events exist for that user, terminiation.

There are only two transitions where a new complete_workout is added, as at least a start_workout and phase_workout progression must be available.














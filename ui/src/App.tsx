import { Routes, Route, Link, useLocation } from "react-router-dom";
import Home from "./pages/Home";
import ExperimentQueue from "./pages/ExperimentQueue";
import AddExperiment from "./pages/AddExperiment";
import EditExperiment from "./pages/EditExperiment";
import PlanPreview from "./pages/PlanPreview";
import WeeklyCalendar from "./pages/WeeklyCalendar";
import CageInterface from "./pages/CageInterface";
import AddCages from "./pages/AddCages";
import BoxRoom from "./pages/BoxRoom";

const NAV_LINKS = [
  { to: "/", label: "Home" },
  { to: "/queue", label: "Queue" },
  { to: "/plan-preview", label: "Plan Preview" },
  { to: "/calendar", label: "Calendar" },
  { to: "/cages", label: "Cages" },
  { to: "/box-room", label: "Box Room" },
];

function NavBar() {
  const location = useLocation();

  return (
    <nav className="bg-bg-secondary border-b border-border px-4 py-2 flex items-center gap-1">
      <span className="text-accent-green font-semibold mr-4 text-sm">
        Experiment Queue
      </span>
      {NAV_LINKS.map((link) => {
        const isActive =
          link.to === "/"
            ? location.pathname === "/"
            : location.pathname.startsWith(link.to);
        return (
          <Link
            key={link.to}
            to={link.to}
            className={`px-3 py-1.5 rounded text-sm transition-colors ${
              isActive
                ? "bg-accent-blue text-white"
                : "text-text-secondary hover:text-text-primary hover:bg-bg-hover"
            }`}
          >
            {link.label}
          </Link>
        );
      })}
    </nav>
  );
}

export default function App() {
  return (
    <div className="min-h-screen bg-bg-primary text-text-primary">
      <NavBar />
      <main className="p-4">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/queue" element={<ExperimentQueue />} />
          <Route path="/add" element={<AddExperiment />} />
          <Route path="/edit/:recordId" element={<EditExperiment />} />
          <Route path="/plan-preview" element={<PlanPreview />} />
          <Route path="/calendar" element={<WeeklyCalendar />} />
          <Route path="/cages" element={<CageInterface />} />
          <Route path="/cages/add" element={<AddCages />} />
          <Route path="/box-room" element={<BoxRoom />} />
        </Routes>
      </main>
    </div>
  );
}

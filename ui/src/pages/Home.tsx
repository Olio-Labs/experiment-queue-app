import { Link } from "react-router-dom";

const CARDS = [
  {
    to: "/queue",
    icon: "📅",
    title: "Experiment Queue",
    description:
      "Schedule experiments, manage the queue, track experiment progress. View weekly calendars and push experiments.",
    color: "text-green-500",
  },
  {
    to: "/cages",
    icon: "🏠",
    title: "Cage Interface",
    description:
      "Manage cage status. Add new cages, edit cage details, and view cage availability.",
    color: "text-orange-500",
  },
  {
    to: "/box-room",
    icon: "📦",
    title: "Box Room",
    description:
      "View box room layout, check cage assignments, and review flagged issues history for each box.",
    color: "text-purple-500",
  },
];

export default function Home() {
  return (
    <div className="max-w-3xl mx-auto mt-12 text-center">
      <h1 className="text-4xl font-bold text-text-primary mb-2">
        Lab Management System
      </h1>
      <p className="text-text-secondary text-lg mb-12">
        Your central hub for experiment scheduling and cage management
      </p>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {CARDS.map((card) => (
          <Link
            key={card.to}
            to={card.to}
            className="bg-bg-secondary border border-border rounded-xl p-8 hover:border-accent-blue hover:-translate-y-1 transition-all group"
          >
            <span className={`text-5xl block mb-4 ${card.color}`}>
              {card.icon}
            </span>
            <h2 className="text-lg font-semibold text-text-primary mb-3 group-hover:text-accent-blue transition-colors">
              {card.title}
            </h2>
            <p className="text-text-secondary text-sm leading-relaxed">
              {card.description}
            </p>
          </Link>
        ))}
      </div>
    </div>
  );
}

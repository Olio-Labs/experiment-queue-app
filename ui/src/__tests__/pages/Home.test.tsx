import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import Home from "../../pages/Home";

function renderHome() {
  return render(
    <MemoryRouter>
      <Home />
    </MemoryRouter>,
  );
}

describe("Home page", () => {
  it("renders the title", () => {
    renderHome();
    expect(screen.getByText("Lab Management System")).toBeInTheDocument();
  });

  it("renders all three navigation cards", () => {
    renderHome();
    expect(screen.getByText("Experiment Queue")).toBeInTheDocument();
    expect(screen.getByText("Cage Interface")).toBeInTheDocument();
    expect(screen.getByText("Box Room")).toBeInTheDocument();
  });

  it("links to the correct routes", () => {
    renderHome();
    const links = screen.getAllByRole("link");
    const hrefs = links.map((a) => a.getAttribute("href"));
    expect(hrefs).toContain("/queue");
    expect(hrefs).toContain("/cages");
    expect(hrefs).toContain("/box-room");
  });
});
